# lakeworks-animal-conform

The Glue Spark job that conforms every shelter feed into the animal domain's event model, and the
Terraform that deploys it. Shelter portals publish the same intakes and outcomes in different
shapes, grains and vocabularies. This job makes them agree and writes one event stream to the
silver layer.

It is one part of the lakeworks platform. Each neighbor holds a part this repo does not:

- [`lakeworks-animal-sources`](https://github.com/datapointchris/lakeworks-animal-sources) declares
  each source: its portal, its shape and its column mapping. This repo reads those declarations and
  holds no code for any one source.
- [`lakeworks-platform-lib`](https://github.com/datapointchris/lakeworks-platform-lib) builds the
  Spark session and runs the write-audit-publish commit. The job imports it at a pinned tag.
- [`lakeworks-platform-core`](https://github.com/datapointchris/lakeworks-platform-core) creates the
  lake and ops buckets and the Glue databases. This repo creates only its own job, role and log
  group.
- [`terraform-aws-lakeworks-naming`](https://github.com/datapointchris/terraform-aws-lakeworks-naming)
  constructs the bucket, role, log group and job names this repo uses.

## Install

```bash
git clone https://github.com/datapointchris/lakeworks-animal-conform
cd lakeworks-animal-conform
uv sync --extra dev    # the job's dependencies, plus pyspark pinned to the Spark Glue runs
```

`infra/` needs Terraform at the version `infra/main.tf` requires. `requires-python` is held at the
Python that Glue runs, because a job built for a higher floor cannot run there.

`lakeworks` resolves from GitHub at a tag, never a branch. A pipeline floating on `main` gets a
different library on every deploy, and the failure looks like a data problem rather than a
dependency one.

## Starting from scratch

```bash
terraform -chdir=infra init -backend=false   # providers and the naming module; touches no state
terraform -chdir=infra validate              # fails if pipeline.yml lacks a key the module reads

# Everything below reaches AWS, as whatever credentials the shell holds.
terraform -chdir=infra init \
  -backend-config="bucket=lakeworks-tfstate-<management-account-id>" \
  -backend-config="key=dev/animal/conform/terraform.tfstate"   # the env here matches -var env
terraform -chdir=infra apply -var env=dev -var artifact_sha=$(git rev-parse --short HEAD)
terraform -chdir=infra output glue_job_name  # the name Glue holds the job under
```

The first two commands need no credentials. `validate` decodes `pipeline.yml` and fails on any key
the module reads that the spec lacks. It does not run the precondition that refuses a job whose
`type` is not `glue_spark`. `apply` runs it.

The backend is partial. `infra/backend.tf` leaves out the bucket and the key, because a backend
block cannot read a variable and both vary. The bucket's name carries an account id, and the key
carries the environment, so both are passed at init. The key's env segment has to match
`-var env`. A `prod` deploy is initialized with `key=prod/animal/conform/terraform.tfstate`, and a
mismatch records one environment's resources in the other's state.

The provider assumes no role. The job lands in the account the shell's credentials resolve to, and
the bucket names the naming module builds carry that account's id. The buckets and databases the
job uses are `lakeworks-platform-core`'s, so that repo is applied first.

`apply` creates the role, its policy, the log group and the Glue job. It uploads nothing. Glue reads
the script from `s3://<ops bucket>/artifacts/animal-conform/<artifact_sha>/jobs/conform.py`.
`--extra-files` stages `<artifact_sha>/sources/` from the same prefix, and the job reads the source
specs from `sources/` in its working directory. Those specs are the YAML files of
`lakeworks-animal-sources`. Both have to be at those paths before the first run. A run whose
`sources/` was never staged raises `FileNotFoundError` rather than conforming zero sources. A run
that conforms nothing and reports success looks exactly like a working one.

## `jobs/conform.py` — every source through one path, with no branch that names a source

Sources disagree three ways, and the job reconciles each without a per-source branch.

| Conflict | Example |
| --- | --- |
| Shape | Austin publishes separate intake and outcome feeds; Sonoma publishes one row carrying both |
| Grain | One row per animal in some feeds, one row per animal-visit in others |
| Vocabulary | `Return to Owner` / `RETURN TO OWNER` / `RTO` are the same outcome |

**The load-bearing claim: `jobs/conform.py` contains no source-specific branch.** Each difference is
data. Shape and column mapping are declared per source in `lakeworks-animal-sources`. Vocabulary is
a row in `dim_outcome_type`. A source that does not fit gets a declaration or a dimension row, never
an `if` naming it. Thirty sources handled by thirty `if` statements is thirty near-identical code
paths that diverge over two years, each with its own retry bug.

The one piece of genuine code is the shape dispatch. It is an enum with a branch per member and no
catch-all. A source declaring a shape with no branch fails the run with a `ValueError`. It never
falls through to whichever shape was written first.

A spelling with no row in the vocabulary table becomes `__unmapped__`, never null. A null is
indistinguishable from a value the source never gave. A run that leaves any outcome type unmapped
logs a warning with the count. The fix for each spelling is a row in `dim_outcome_type`.

The write goes through write-audit-publish. The events land on an Iceberg branch, and the branch is
published only if two assertions hold: the grain of animal key, event time and event type is
unique, and rows arrived. A failed audit leaves the branch in place for inspection and the published
table untouched.

## `pipeline.yml` — the spec Terraform reads rather than restates

`infra/main.tf` reads `pipeline.yml` with `yamldecode`. A second declaration of the same facts in
HCL is a second thing to keep in sync. It is the one that drifts, because nothing runs the HCL copy
against real data.

Terraform reads the pipeline's name, domain and owner, its first job, and its `reads` and `writes`.
From the job it takes the entrypoint, the Glue version, the worker type, the worker count per
environment, the timeout and the concurrency limit, and it checks the job's `type`. Each table
name carries an `{env}` placeholder that `-var env` fills, so one spec drives both environments.
Nothing in this repo reads the `schedule`, `orchestration`, `maintenance`, `alerts` or `contracts`
blocks.

**The Glue catalog permissions are generated from the declared `reads` and `writes`.** The job can
read only the tables it declares reading. It can create or update only the tables it declares
writing. Adding a table to the spec grants access to its database with no other edit. Least
privilege stops being discipline and becomes a property of the system.

S3 access is coarser than the catalog. The role reads the whole lake bucket and writes anywhere
under `warehouse/`. The catalog is where the declared boundary holds.

Hand-writing a policy statement to unblock something breaks that property silently. The job then
holds access the spec does not account for, and nothing reports it.

## `infra/` — the Terraform lives beside the code it deploys

One PR changes the job and its infrastructure together, and the deploy is atomic. The arrangement
fits because this pipeline owns its Glue job, its role and its state exclusively. The lakeworks
design calls it Type A.

The design puts the `sensor` domain the other way, with its Terraform in a repo of its own. There,
one stream is designed to feed two jobs. Beside the code, that stream would have no owner: in either
consumer's repo it makes the other depend on a peer, and in both it is defined twice. The
comparison between the two arrangements is itself a deliverable. This repo is not converged toward
the other one for the sake of consistency.

| | Terraform beside the code | Terraform in its own repo |
| --- | --- | --- |
| A code and infrastructure change | One PR, deployed atomically | Two PRs, and the order matters |
| A module version bump across pipelines | One PR per pipeline | One PR |
| A resource two pipelines share | Has no owner | The infra repo owns it |
| Blast radius of an apply | One pipeline | Every pipeline in the repo |

`artifact_sha` must be a hex commit sha, and a branch or tag fails validation. The S3 path is the
only versioning Glue offers. A job whose script is `latest.py` cannot be rolled back and cannot be
reproduced. An apply with an earlier sha points the job back at that sha's script.

Nothing here reads another repo's Terraform state. The bucket, role, log group and job names come
from the naming module, built from the environment, the domain, the pipeline and the account id.
A `terraform_remote_state` data source would hand this repo the whole platform state file in
exchange for a single value.
