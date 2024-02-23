data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}
#role
resource "aws_iam_role" "iam_for_lambda" {
  name_prefix = "role-lambdas"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}



data "aws_iam_policy_document" "s3_document" {
  statement {

    
    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.json_bucket.arn}/*",
    ]
  }
}
resource "aws_iam_policy" "s3_policy" {
    name_prefix = "s3-policy-for-lambdas"
    policy = data.aws_iam_policy_document.s3_document.json
}
#attach policy to the role
resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role = aws_iam_role.iam_for_lambda.name
    policy_arn = aws_iam_policy.s3_policy.arn
}




data "aws_iam_policy_document" "cw_document" {
  statement {

    effect = "Allow"
    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {

    effect = "Allow"
    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.parquet_lambda_name}:*",
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.json_to_parquet_lambda}:*",
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.dummy_json_lambda}:*"
    ]
  }

  # allow lambda to upload files to s3
  statement {

    effect = "Allow"
    actions = [ "s3:*" ]

    resources = [
       "arn:aws:s3:::*"
    ]
  }

}

resource "aws_iam_policy" "cw_policy" {
    name_prefix = "cw-policy-${var.parquet_lambda_name}"
    policy = data.aws_iam_policy_document.cw_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
    role = aws_iam_role.iam_for_lambda.name
    policy_arn = aws_iam_policy.cw_policy.arn
}

resource "aws_iam_policy" "lambda_secrets_policy" {
  name        = "LambdaSecretsPolicy"
  description = "IAM policy for Lambda to access Secrets Manager"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "secretsmanager:GetSecretValue"# the secret values stored on your aws account
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_secrets_attachment" {
  role       = aws_iam_role.iam_for_lambda.name
  policy_arn = aws_iam_policy.lambda_secrets_policy.arn
}

# allow lambda to access parameter store
resource "aws_iam_policy" "lambda_ssm_policy" {
  name        = "LambdaSSMPolicy"
  description = "IAM policy for Lambda to access SSM parameters"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ssm:GetParameter", "ssm:PutParameter"] 
      Resource = "arn:aws:ssm:eu-west-2:*:parameter/time"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_ssm_attachment" {
  role       =  aws_iam_role.iam_for_lambda.name
  policy_arn = aws_iam_policy.lambda_ssm_policy.arn
}

