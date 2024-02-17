#lambda json->parket
resource "aws_lambda_function" "lambda_parquet" {
  filename      = "${path.module}/../json_to_parquet.zip"
  function_name = "${var.parquet_lambda_name}"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "json_to_parquet.json_to_parquet"
  runtime = "python3.10"
  timeout          = 180
  layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python310:8"]
}

# lambda triger  
resource "aws_lambda_permission" "allow_s3_json" {
  statement_id  = "Allow_s3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_parquet.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.json_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}

#lambda Parquet -> Json log to cloud watch
resource "aws_lambda_function" "lambda_josn_to_parquet" {
  filename      = "${path.module}/../parquet_to_json.zip"
  function_name = "${var.json_to_parquet_lambda}"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "parquet_to_json.parquet_to_json"
  runtime = "python3.10"
  timeout          = 180
  layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python310:8"]
}
resource "aws_lambda_permission" "allow_s3_parquet" {
  statement_id  = "Allow_s3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_josn_to_parquet.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.parquet_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}

#Create dummy Json lambda
#lambda Parquet -> Json log to cloud watch
resource "aws_lambda_function" "dummy_lambda" {
  filename      = "${path.module}/../ingestion.zip"
  function_name = "${var.dummy_json_lambda}"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "ingestion.ingestion"
  runtime = "python3.10"
  timeout          = 180
  layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python310:8"]
}

resource "aws_cloudwatch_event_rule" "scheduler" {
    name_prefix = "dummy_lambda-"
    schedule_expression = "rate(1 minute)"
}

resource "aws_lambda_permission" "allow_scheduler" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.dummy_lambda.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.scheduler.name
  arn       = aws_lambda_function.dummy_lambda.arn
}