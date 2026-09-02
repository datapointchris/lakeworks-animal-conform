# CLAUDE.md

Guidance for Claude Code working in this repository.

Read the README first. It carries the three ways sources disagree, why the Terraform lives beside
the code here, and why the spec drives the infrastructure rather than being restated in HCL.

## No source-specific branch in the job

`jobs/conform.py` contains none, and that is the claim the whole design rests on. Shape and mapping
are declared in the sources repo; vocabulary is a row in `dim_outcome_type`.

When a source does not fit, the fix is a declaration or a dimension row — never an `if` naming that
source. Thirty sources handled by thirty branches is thirty near-identical paths that diverge over
two years, each acquiring its own retry bug.

The one genuine piece of code is the shape dispatch. It is an enum with a branch per member so a
new portal platform fails loudly instead of falling through a default. Do not add a catch-all
`else` to it.

## The spec is read, not restated

`infra/main.tf` reads `pipeline.yml` with `yamldecode`. Do not copy a value out of the spec into
HCL, even one — a second declaration of the same fact is the one that drifts, because nothing runs
the HCL copy against real data.

This is what makes the IAM policy generated from the declared `reads` and `writes`. A job gets read
access to exactly what it claims to read. Hand-writing a policy statement to unblock something
breaks that property silently: the job then has access the spec does not account for, and nothing
reports it.

## `artifact_sha` refuses a branch or tag

It is validated as a hex sha, on purpose. The S3 path is the only versioning Glue offers, so a job
whose script is `latest.py` cannot be rolled back and cannot be reproduced. Do not relax the
validation to make a deploy command shorter.

```bash
terraform -chdir=infra init
terraform -chdir=infra apply -var env=dev -var artifact_sha=$(git rev-parse --short HEAD)
```

## Topology here is deliberate and not the only one

Terraform lives in `infra/` beside the code it provisions, so one PR changes both and the deploy is
atomic. Another domain in the platform splits infrastructure into its own repo, because its
pipelines share a resource that would otherwise have no owner. The comparison between the two is
itself a deliverable, so do not converge this repo toward the other on the grounds of consistency.

## Buckets and database names come from SSM

Nothing here reads another repo's Terraform state. The platform publishes
`/lakeworks/{env}/platform/...` and consumers read the one parameter they need. Adding a
`terraform_remote_state` data source would hand this repo the whole platform state file in place of
a single value.
