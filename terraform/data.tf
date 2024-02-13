data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# Archive a single file.
data "archive_file" "init" {
  type        = "zip"
  source_file = "${path.module}/../src/json_to_parquet.py"
  output_path = "${path.module}/../json_to_parquet.zip"
}

data "archive_file" "init_parquet_to_json_lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/parquet_to_json.py"
  output_path = "${path.module}/../parquet_to_json.zip"
}

data "archive_file" "init_dummy_json" {
  type        = "zip"
  source_file = "${path.module}/../src/create_json.py"
  output_path = "${path.module}/../create_json.zip"
}
