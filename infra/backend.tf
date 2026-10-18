terraform {
  # Partial config. A backend block cannot read a variable, and both missing values vary: the
  # bucket's name carries the management account id, and the key carries the environment. Both
  # are supplied at init —
  #   terraform init -backend-config="bucket=lakeworks-tfstate-<management-account-id>" \
  #     -backend-config="key=<env>/animal/conform/terraform.tfstate"
  #
  # The key derives from the repo name with the env inserted, so no mapping table is needed. Its
  # env segment has to match the -var env that plan and apply receive, or one environment's
  # resources are recorded in the other's state.
  backend "s3" {
    region       = "us-east-2"
    use_lockfile = true # S3 native locking; no DynamoDB table
  }
}
