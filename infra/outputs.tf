output "glue_job_name" {
  description = "The name AWS holds the Glue job under, for starting a run or reading its history."
  value       = aws_glue_job.conform.name
}

output "task_role_arn" {
  value = aws_iam_role.task.arn
}
