terraform {
  backend "s3" {
    # State path derives from the repo name with the env inserted, so no mapping table is needed:
    # lakeworks-animal-conform owns dev/animal/conform/terraform.tfstate.
    key          = "dev/animal/conform/terraform.tfstate"
    region       = "us-east-2"
    use_lockfile = true # S3 native locking; no DynamoDB table
  }
}
