
resource "aws_s3_bucket" "parquet_bucket" {
  bucket = "processed-zone-895623xx35"
  force_destroy = true
}

resource "aws_s3_bucket" "json_bucket" {
  bucket = "ingestion-zone-895623xx35"
  force_destroy = true
}


resource "aws_s3_bucket_notification" "json_bucket_notification" {
  bucket = aws_s3_bucket.json_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_parquet.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3_json]
}

resource "aws_s3_bucket_notification" "parquet_bucket" {
  bucket = aws_s3_bucket.parquet_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_josn_to_parquet.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3_parquet]
}
