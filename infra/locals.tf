locals {
  account = data.aws_caller_identity.current.account_id

  lake_bucket_arn = "arn:aws:s3:::${module.naming.lake_bucket}"
}
