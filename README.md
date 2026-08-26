# lakeworks-animal-conform

Conform every shelter feed into the domain event model. The hardest ordinary problem in the
department, and the one most portfolios skip.

**Topology: Type A** — the Terraform for this pipeline lives in `infra/`, beside the code it
provisions. One PR changes both and the deploy is atomic. The `sensor` domain deliberately does the
opposite; the comparison is a deliverable. See
[terraform.md](https://github.com/datapointchris/lakeworks-platform-docs/blob/main/docs/terraform.md).

## The problem

Sources disagree three ways, and each has to be reconciled without a per-source branch in the code.

| Conflict | Example |
| --- | --- |
| Shape | Austin publishes separate intake and outcome feeds; Sonoma publishes one row carrying both |
| Grain | One row per animal in some feeds, one row per animal-visit in others |
| Vocabulary | `Return to Owner` / `RETURN TO OWNER` / `RTO` are the same outcome |

**The load-bearing claim: `jobs/conform.py` contains no source-specific branch.** Shape and mapping
are declared in `lakeworks-animal-sources`; vocabulary is a row in `dim_outcome_type`. Thirty
sources handled by thirty `if` statements is thirty near-identical code paths that diverge over two
years, each with its own retry bug.

The one thing that is genuinely code is the shape dispatch, and it is an enum with a branch per
member so a sixth portal platform fails loudly rather than falling through.

## Layout

```text
pipeline.yml     the spec — schedule, workers, reads, writes, audit assertions
jobs/conform.py  the PySpark job
infra/           the Terraform root module for this pipeline
```

## The spec drives the infrastructure

`infra/main.tf` reads `pipeline.yml` with `yamldecode` rather than restating it. A second
declaration of the same facts in HCL is a second thing to keep in sync, and it is the one that
drifts, because nothing runs the HCL copy against real data.

The consequence worth having: **the IAM policy is generated from the declared `reads` and `writes`.**
A job gets read access to exactly what it says it reads and write access to exactly what it says it
writes. Least privilege stops being discipline and becomes a property of the system.

## Deploying

```bash
terraform -chdir=infra init
terraform -chdir=infra apply -var env=dev -var artifact_sha=$(git rev-parse --short HEAD)
```

`artifact_sha` is validated as a hex sha and refuses a branch or tag. A Glue job whose script is
`latest.py` cannot be rolled back and cannot be reproduced, and the S3 path is the only versioning
Glue offers.
