variable "env" {
  description = "Deployment environment. A repo carries no environment; it deploys to both."
  type        = string

  validation {
    condition     = contains(["dev", "prod"], var.env)
    error_message = "env must be dev or prod."
  }
}

variable "region" {
  description = "AWS region. One region, and an SCP denies the others."
  type        = string
  default     = "us-east-2"
}

variable "artifact_sha" {
  description = "Commit sha of the job artifact in the ops bucket. Never `latest` — a Glue job whose script is latest.py cannot be rolled back and cannot be reproduced, and the S3 path is the only versioning Glue offers."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-f]{7,40}$", var.artifact_sha))
    error_message = "artifact_sha must be a hex commit sha, not a branch or tag."
  }
}
