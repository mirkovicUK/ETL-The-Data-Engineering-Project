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
    effect = "Allow"
    actions = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:ListAllMyBuckets"]

    resources = [
      "${aws_s3_bucket.json_bucket.arn}/*",
      "${aws_s3_bucket.parquet_bucket.arn}/*"
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
  # statement {

  #   effect = "Allow"
  #   actions = [ "s3:*" ]

  #   resources = [
  #      "arn:aws:s3:::*"
  #   ]
  # }

  #allowe lamda to read parameter store
  statement {

    effect = "Allow"
    actions = [ "ssm:GetParameter", "ssm:PutParameter" ]

    resources = [
       "arn:aws:ssm:eu-west-2:381492264258:parameter/time"
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


