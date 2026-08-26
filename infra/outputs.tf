output "glue_job_name" {
  description = "Read by dectl to resolve this pipeline's alias to its real AWS name."
  value       = aws_glue_job.conform.name
}

output "task_role_arn" {
  value = aws_iam_role.task.arn
}
