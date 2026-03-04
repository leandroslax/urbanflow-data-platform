output "datalake_bucket" {
  value = aws_s3_bucket.datalake.bucket
}

output "msk_serverless_cluster_arn" {
  value = aws_msk_serverless_cluster.urbanflow.arn
}
