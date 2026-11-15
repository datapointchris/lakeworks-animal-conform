# lakeworks-animal-conform

The README carries the three ways sources disagree, why the Terraform lives beside the code here,
and why the spec drives the infrastructure rather than being restated in HCL.

## No source-specific branch in the job

`jobs/conform.py` contains none. When a source does not fit, the fix is a declaration in the
sources repo or a row in a vocabulary table, never an `if` naming that source. The shape dispatch
is an enum with a branch per member. Do not add a catch-all case to it.

README.md's section on `jobs/conform.py` says why.

## Every external effect in the job has one door

Anything `jobs/conform.py` reads from or writes to outside the process goes through one named
function, and every caller uses it. That function is where a timeout, a retry or a test stand-in
attaches, once.

- **The Spark session** comes only from `lakespark.session`, in the `lakeworks` library.
- **Catalog reads** go only through `read_catalog_table`. `read_bronze` and the vocabulary lookups
  in `run()` call it; nothing else calls `spark.table`.
- **The staged source specs** are read from disk only by `load_source_specs`.
- **The silver write** goes only through `iceberg.write_audit_publish`, in the `lakeworks` library.

There are no sanctioned exceptions. A new read or write gets a call through one of these, or a
new door named here beside them.

## The spec is read, not restated

`infra/main.tf` reads `pipeline.yml` with `yamldecode`. Do not copy a value out of the spec into
HCL, even one. Do not hand-write an IAM policy statement to unblock something: the Glue catalog
statements are generated from the spec's `reads` and `writes`.

`jobs/conform.py` does not read `pipeline.yml`. It names each table it reads or writes as a literal,
spelled as in the spec's `reads` and `writes` with the `{env}` placeholder, and resolves it with
`lakespark.resolve_table`. A table renamed in the spec is renamed in `jobs/conform.py` too, or the
job reaches a table its grant does not cover.

README.md's section on `pipeline.yml` says why.

## `artifact_sha` refuses a branch or tag

Do not relax its hex-sha validation to make a deploy command shorter. README.md's section on
`infra/` says why, and its section on starting from scratch holds the init and apply commands.

## Topology here is deliberate and not the only one

Terraform lives in `infra/` beside the code it provisions. Do not move it to a repo of its own, or
otherwise converge this repo toward another domain's arrangement, for the sake of consistency.
README.md's section on `infra/` says why.

## Bucket names come from the naming module, database names from the spec

`terraform-aws-lakeworks-naming` builds the lake and ops bucket names from the environment and the
account id, the same module and inputs `lakeworks-platform-core` creates the buckets with. Keep this
repo's pin on the naming module in step with platform-core's. A pin that builds a different name
points the job at a bucket nobody created. Never concatenate a resource name in HCL here; the naming
module is the one place a name is built.

Glue database names are the first segment of each table identifier in the spec's `reads` and
`writes`, with `{env}` filled.

Nothing here reads SSM or another repo's Terraform state. The platform publishes the same names
under `/lakeworks/{env}/platform/...` for consumers that cannot derive them. Do not add a
`terraform_remote_state` data source. README.md's section on `infra/` says why.
