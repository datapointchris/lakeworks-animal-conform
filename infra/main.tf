terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = module.naming.tags
  }
}

data "aws_caller_identity" "current" {}

# The pipeline spec is the source of truth, and Terraform reads it rather than restating it. A
# second declaration of the same facts in HCL is a second thing to keep in sync, and it is the one
# that drifts — nothing runs the HCL copy against real data.
locals {
  spec = yamldecode(file("${path.module}/../pipeline.yml"))
  job  = local.spec.jobs[0]

  # `reads` and `writes` carry an {env} placeholder so one spec drives both environments.
  reads  = [for t in local.spec.reads : replace(t, "{env}", var.env)]
  writes = [for t in local.spec.writes : replace(t, "{env}", var.env)]

  # A Glue database name is the table identifier's first segment. Deriving the set this way means
  # a new table in the spec grants access to its database with no other edit.
  read_databases  = toset([for t in local.reads : split(".", t)[0]])
  write_databases = toset([for t in local.writes : split(".", t)[0]])

  workers = local.job.workers[var.env]
}

module "naming" {
  source = "git::https://github.com/datapointchris/terraform-aws-lakeworks-naming.git?ref=v0.1.0"

  env        = var.env
  domain     = local.spec.domain
  pipeline   = replace(local.spec.name, "${local.spec.domain}-", "")
  layer      = "silver"
  owner      = local.spec.owner
  account_id = data.aws_caller_identity.current.account_id
}

# ---------------------------------------------------------------- identity

resource "aws_iam_role" "task" {
  name = module.naming.task_role

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Least privilege stops being discipline and becomes a property of the system: the policy is
# generated from what the spec declares, so a job cannot read a table it did not say it reads.
data "aws_iam_policy_document" "task" {
  statement {
    sid       = "ReadLakeData"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [local.lake_bucket_arn, "${local.lake_bucket_arn}/*"]
  }

  statement {
    sid       = "WriteWarehouse"
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${local.lake_bucket_arn}/warehouse/*"]
  }

  statement {
    sid    = "ReadDeclaredTables"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartitions",
    ]
    resources = concat(
      ["arn:aws:glue:${var.region}:${local.account}:catalog"],
      [for db in local.read_databases : "arn:aws:glue:${var.region}:${local.account}:database/${db}"],
      [for t in local.reads : "arn:aws:glue:${var.region}:${local.account}:table/${replace(t, ".", "/")}"],
    )
  }

  # Write needs UpdateTable because an Iceberg commit repoints the table's metadata location, and
  # branch creation for write-audit-publish is the same operation.
  statement {
    sid    = "WriteDeclaredTables"
    effect = "Allow"
    actions = [
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetTable",
      "glue:GetDatabase",
    ]
    resources = concat(
      ["arn:aws:glue:${var.region}:${local.account}:catalog"],
      [for db in local.write_databases : "arn:aws:glue:${var.region}:${local.account}:database/${db}"],
      [for t in local.writes : "arn:aws:glue:${var.region}:${local.account}:table/${replace(t, ".", "/")}"],
    )
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:CreateLogGroup"]
    resources = ["arn:aws:logs:${var.region}:${local.account}:log-group:${module.naming.log_group}*"]
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "declared-access"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}

# ---------------------------------------------------------------- compute

resource "aws_cloudwatch_log_group" "job" {
  name = module.naming.log_group

  # Never left unset. CloudWatch bills on ingested volume, and a chatty Spark job at DEBUG can cost
  # more in logs than in compute.
  retention_in_days = var.env == "prod" ? 30 : 14
}

resource "aws_glue_job" "conform" {
  name              = module.naming.glue_job
  role_arn          = aws_iam_role.task.arn
  glue_version      = local.job.glue_version
  worker_type       = local.job.worker_type
  number_of_workers = local.workers
  timeout           = local.job.timeout_minutes

  # One run at a time. A schedule that fires while the previous run is still going would otherwise
  # stack executions and write the same rows twice.
  execution_property {
    max_concurrent_runs = local.job.max_concurrent_runs
  }

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${module.naming.ops_bucket}/artifacts/${local.spec.name}/${var.artifact_sha}/${local.job.entrypoint}"
  }

  default_arguments = {
    "--enable-glue-datacatalog"          = "true"
    "--datalake-formats"                 = "iceberg"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--LAKEWORKS_TARGET"                 = "glue"
    "--LAKEWORKS_WAREHOUSE"              = "s3://${module.naming.lake_bucket}/warehouse/"
    "--extra-files"                      = "s3://${module.naming.ops_bucket}/artifacts/${local.spec.name}/${var.artifact_sha}/sources/"
  }

  lifecycle {
    precondition {
      condition     = local.job.type == "glue_spark"
      error_message = "This root module provisions a Glue Spark job; pipeline.yml declares ${local.job.type}."
    }
  }
}
