locals {
  account = data.aws_caller_identity.current.account_id

  lake_bucket_arn = "arn:aws:s3:::${module.naming.lake_bucket}"

  # Bucket and key prefix of this commit's job artifact, without a scheme. The script, the staged
  # files and the task role's read grant are all built from it, so they cannot point apart.
  artifact_prefix = "${module.naming.ops_bucket}/artifacts/${local.spec.name}/${var.artifact_sha}"
}
