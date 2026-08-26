"""Conform every shelter feed into the domain event model.

The hardest ordinary problem in this department. Sources disagree three ways:

  1. Shape    — some publish separate intake and outcome feeds, some publish one row carrying both.
  2. Grain    — one row per animal in some feeds, one row per animal-visit in others.
  3. Vocabulary — `Return to Owner` / `RETURN TO OWNER` / `RTO` are the same outcome.

The load-bearing claim about this module: **it contains no source-specific branch.** Every
difference above is expressed as data — a `mapping` block and a `shape` in the source YAML, and a
row in `dim_outcome_type` — because thirty sources handled by thirty `if` statements is thirty
near-identical code paths that diverge over two years, each with its own retry bug.

The one thing that is genuinely code is the shape dispatch, and it is an enum with a branch per
member so that adding a sixth portal platform fails loudly rather than falling through to whatever
was written first.
"""

import enum
import logging
import pathlib

import yaml
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql import window as sw

from lakeworks import iceberg
from lakeworks import spark as lakespark

log = logging.getLogger(__name__)

EVENT_INTAKE = 'intake'
EVENT_OUTCOME = 'outcome'

UNMAPPED = '__unmapped__'
"""Vocabulary values with no mapping land here rather than being silently coerced. A growing
unmapped bucket is alarmed on — silently absorbing an unknown outcome type is how a whole category
disappears from a report without anyone noticing."""


class Shape(enum.Enum):
    """How a source lays out intake and outcome relative to each other."""

    TWO_FEED = 'two_feed'
    """Separate intake and outcome datasets, joinable on a key. Austin."""

    ONE_ROW = 'one_row'
    """One row carrying both. Sonoma, Long Beach."""


# ---------------------------------------------------------------- impure: read the world


def load_source_specs(source_dir: pathlib.Path) -> list[dict]:
    """Read every source YAML.

    Glue's `--extra-files` and EMR's `--files` both stage additional files into the job's working
    directory, so this is an ordinary local read in every deployment target. Reaching through
    PySpark's JVM handle for a Hadoop filesystem would work and is strictly more machinery for a
    file that is already on local disk.

    Args:
        source_dir: Directory holding one YAML per source.

    Returns:
        Parsed specs for enabled sources only.

    Raises:
        FileNotFoundError: If the directory was not staged. Not defaulted to an empty list — a run
            that conforms zero sources and reports success is indistinguishable from a working one.
    """
    if not source_dir.is_dir():
        raise FileNotFoundError(f'Source spec directory {source_dir} was not staged. Check --extra-files on the job definition.')

    specs = [yaml.safe_load(path.read_text()) for path in sorted(source_dir.glob('*.yml'))]
    enabled = [spec for spec in specs if spec['enabled']]
    log.info(f'loaded {len(enabled)} enabled source specs of {len(specs)} found in {source_dir}')
    return enabled


def read_bronze(spark: SparkSession, source_id: str, feed: str) -> DataFrame:
    """Read one source's bronze feed.

    Args:
        spark: Active session.
        source_id: Source identifier, e.g. `animal.austin`.
        feed: Feed name within the source, e.g. `intake`, `outcome`, `combined`.

    Returns:
        The bronze rows for that source and feed.
    """
    return spark.table('lakeworks_animal_bronze.shelter_feed').where((sf.col('source_id') == source_id) & (sf.col('feed') == feed))


# ---------------------------------------------------------------- pure: the transformation


def project(frame: DataFrame, mapping: dict[str, str]) -> DataFrame:
    """Rename source columns to domain columns, dropping everything unmapped.

    Args:
        frame: Raw bronze rows with the source's own column names.
        mapping: Domain column name to source column name.

    Returns:
        A frame carrying only domain columns, plus the ingestion metadata every bronze row has.
    """
    projected = [sf.col(f'`{source_col}`').alias(domain_col) for domain_col, source_col in mapping.items()]
    metadata = [sf.col('_ingested_at'), sf.col('_source_run_id'), sf.col('source_id')]
    return frame.select(*projected, *metadata)


def to_events_from_two_feed(intake: DataFrame, outcome: DataFrame, join_key: str) -> DataFrame:
    """Turn separate intake and outcome feeds into one event stream.

    The join is not on the key alone. An animal can be surrendered more than once, so a naive join
    on `animal_id` produces a cross product of every intake against every outcome for that animal.
    Each outcome is matched to the most recent intake preceding it.

    Args:
        intake: Projected intake rows.
        outcome: Projected outcome rows.
        join_key: Domain column holding the source's animal identifier.

    Returns:
        One row per event, tagged with `event_type`.
    """
    intake_events = intake.withColumn('event_type', sf.lit(EVENT_INTAKE))

    # Sequence each animal's intakes so an outcome can be attributed to the visit it ended, rather
    # than to the animal as a whole.
    visit = sw.Window.partitionBy(join_key).orderBy('event_at')
    intake_sequenced = intake.withColumn('visit_seq', sf.row_number().over(visit)).select(
        sf.col(join_key), sf.col('event_at').alias('intake_at'), 'visit_seq'
    )

    matched = (
        outcome.alias('o').join(intake_sequenced.alias('i'), on=join_key, how='left').where(sf.col('i.intake_at') <= sf.col('o.event_at'))
    )
    latest_preceding = sw.Window.partitionBy(join_key, 'o.event_at').orderBy(sf.col('i.intake_at').desc())
    outcome_events = (
        matched.withColumn('rn', sf.row_number().over(latest_preceding))
        .where(sf.col('rn') == 1)
        .drop('rn', 'intake_at')
        .withColumn('event_type', sf.lit(EVENT_OUTCOME))
    )

    return intake_events.unionByName(outcome_events, allowMissingColumns=True)


def to_events_from_one_row(combined: DataFrame) -> DataFrame:
    """Split one-row-per-visit records into intake and outcome events.

    A null outcome timestamp means the animal is still in care. That must produce an intake event
    and no outcome event — emitting an outcome with a null timestamp would make every
    still-in-care animal look like a completed visit downstream.

    Args:
        combined: Projected rows carrying both intake and outcome columns.

    Returns:
        One row per event.
    """
    shared = [c for c in combined.columns if not c.startswith(('intake_', 'outcome_'))]

    intake_events = combined.select(
        *shared,
        sf.col('intake_at').alias('event_at'),
        sf.col('intake_type_raw'),
        sf.col('intake_condition_raw'),
        sf.lit(None).cast('string').alias('outcome_type_raw'),
        sf.lit(None).cast('string').alias('outcome_subtype_raw'),
        sf.lit(EVENT_INTAKE).alias('event_type'),
    )

    outcome_events = combined.where(sf.col('outcome_at').isNotNull()).select(
        *shared,
        sf.col('outcome_at').alias('event_at'),
        sf.lit(None).cast('string').alias('intake_type_raw'),
        sf.lit(None).cast('string').alias('intake_condition_raw'),
        sf.col('outcome_type_raw'),
        sf.col('outcome_subtype_raw'),
        sf.lit(EVENT_OUTCOME).alias('event_type'),
    )

    return intake_events.unionByName(outcome_events)


def normalise_vocabulary(events: DataFrame, vocabulary: DataFrame, raw_column: str, out_column: str) -> DataFrame:
    """Map a source's free-text vocabulary onto the controlled one.

    A left join, not a UDF and not a chain of `when`. The mapping is a table so a new source's
    spelling is a row, and the unmapped bucket is a query rather than a code review.

    Args:
        events: Event rows.
        vocabulary: Controlled vocabulary with `source_id`, `raw_value`, `canonical_value`.
        raw_column: Column holding the source's spelling.
        out_column: Column to write the canonical value into.

    Returns:
        Events with the canonical column added. Unmatched values become UNMAPPED, never null —
        a null is indistinguishable from a genuinely absent value.
    """
    lookup = vocabulary.select(
        sf.col('source_id'),
        sf.upper(sf.trim(sf.col('raw_value'))).alias('_raw_key'),
        sf.col('canonical_value'),
    )
    return (
        events.withColumn('_raw_key', sf.upper(sf.trim(sf.col(raw_column))))
        # Broadcast is explicit: the vocabulary is a few hundred rows and the optimizer's threshold
        # is a config, not a guarantee. This join must never become a SortMergeJoin.
        .join(sf.broadcast(lookup), on=['source_id', '_raw_key'], how='left')
        .withColumn(
            out_column,
            sf.when(sf.col(raw_column).isNull(), sf.lit(None).cast('string')).otherwise(
                sf.coalesce(sf.col('canonical_value'), sf.lit(UNMAPPED))
            ),
        )
        .drop('_raw_key', 'canonical_value')
    )


def surrogate_key(*columns: str) -> Column:
    """Build a stable surrogate key from natural key columns.

    A hash rather than a monotonically increasing id, because the key must be reproducible across
    reruns and across a full backfill. An incrementing id makes a backfill assign different keys to
    the same animals, which breaks every fact table that already references them.

    Args:
        *columns: Natural key columns, in a fixed order.

    Returns:
        A 64-bit key column.
    """
    return sf.xxhash64(sf.concat_ws('||', *[sf.coalesce(sf.col(c), sf.lit('')) for c in columns]))


def deduplicate(events: DataFrame) -> DataFrame:
    """Keep the most recently ingested copy of each event.

    Sources republish. The same event arriving twice with different ingestion timestamps is normal,
    and a bronze layer that appends rather than merges is what makes it visible here.

    Args:
        events: Event rows, possibly containing duplicates.

    Returns:
        One row per (animal_key, event_at, event_type).
    """
    latest = sw.Window.partitionBy('animal_key', 'event_at', 'event_type').orderBy(sf.col('_ingested_at').desc())
    return events.withColumn('_rn', sf.row_number().over(latest)).where(sf.col('_rn') == 1).drop('_rn')


# ---------------------------------------------------------------- impure: dispatch and write


def conform_source(spark: SparkSession, spec: dict) -> DataFrame:
    """Produce the domain event stream for one source.

    Args:
        spark: Active session.
        spec: Parsed source YAML.

    Returns:
        Domain events for that source.

    Raises:
        ValueError: If the source declares a shape with no branch here. Deliberately not a
            fallthrough — a new portal platform must fail loudly rather than be silently handled as
            whichever shape happened to be written first.
    """
    shape = Shape(spec['shape'])
    mapping = spec['mapping']
    source_id = spec['source_id']

    match shape:
        case Shape.TWO_FEED:
            intake = project(read_bronze(spark, source_id, 'intake'), mapping['intake'])
            outcome = project(read_bronze(spark, source_id, 'outcome'), mapping['outcome'])
            return to_events_from_two_feed(intake, outcome, spec['join_key'])
        case Shape.ONE_ROW:
            combined = project(read_bronze(spark, source_id, 'combined'), mapping['combined'])
            return to_events_from_one_row(combined)

    raise ValueError(f'Source {source_id} declares shape {shape} with no conformance branch.')


def run() -> None:
    """Conform every enabled source and publish through write-audit-publish."""
    spark = lakespark.session('animal-conform')
    specs = load_source_specs(pathlib.Path('sources'))

    outcome_vocab = spark.table('lakeworks_platform_gold.dim_outcome_type')
    breed_vocab = spark.table('lakeworks_platform_gold.dim_breed')

    per_source = [conform_source(spark, spec) for spec in specs]
    events = per_source[0]
    for frame in per_source[1:]:
        events = events.unionByName(frame, allowMissingColumns=True)

    events = (
        events.withColumn('animal_key', surrogate_key('source_id', 'animal_id'))
        .transform(lambda f: normalise_vocabulary(f, outcome_vocab, 'outcome_type_raw', 'outcome_type'))
        .transform(lambda f: normalise_vocabulary(f, breed_vocab, 'breed_raw', 'breed'))
        .withColumn('_conformed_run_id', sf.lit(lakespark.run_id()))
    )
    events = deduplicate(events)

    target = 'lakeworks_animal_silver.animal_event'
    assertions = [
        iceberg.grain_is_unique('animal_key', 'event_at', 'event_type'),
        iceberg.rows_arrived(),
    ]

    iceberg.stamp_run_id(spark, target)
    with iceberg.write_audit_publish(spark, target, assertions) as staged:
        events.writeTo(staged).append()

    unmapped = events.where(sf.col('outcome_type') == UNMAPPED).count()
    if unmapped > 0:
        log.warning(f'{unmapped} events carry an unmapped outcome type — dim_outcome_type needs a row')


if __name__ == '__main__':
    run()
