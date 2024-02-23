resource "aws_sns_topic" "error_alerts" {
  name = "lambda-error-alerts"
}

resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.error_alerts.arn
  protocol  = "email"
  endpoint  = "beekeepers_error_log@mail.com"
  
}
resource "aws_cloudwatch_log_metric_filter" "AccessDeniedException_error" {
  name           = "AccessDeniedException_error"
  pattern        = "AccessDeniedException" #string to search for
  log_group_name = "/aws/lambda/${var.dummy_json_lambda}"
  metric_transformation {
    name      = "AccessDeniedExceptionErrorCount"
    namespace = "AccessDeniedExceptionLambdaLoadMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "AccessDeniedException_error_alarm" {
  alarm_name          = "not_authorized_to_perform_error_alarm" 
  metric_name         = "AccessDeniedExceptionErrorCount" # must match metric transformation name
  evaluation_periods  = 1
  period              = 10
  threshold           = 1
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  namespace           = "AccessDeniedExceptionLambdaLoadMetrics" #must match metric transformation namespace
  alarm_actions       = [aws_sns_topic.error_alerts.arn]
  alarm_description   = "access denied"
}

# resource "aws_cloudwatch_log_metric_filter" "pg8000_error" {
#   name           = "pg8000 error"
#   pattern        = "Not pg8000 connection"
#   log_group_name = "/aws/lambda/${var.dummy_json_lambda}" # might be wrong name, not sure of lambda name

#   metric_transformation {
#     name      = "pg8000 errorCount"
#     namespace = "pg8000LambdaErrorMetrics"
#     value     = "1"
#   }
# }
 

# resource "aws_cloudwatch_metric_alarm" "pg8000_error_alarm" {
#   alarm_name          = "pg8000_error_alarm"
#   metric_name         = "ErrorCount"
#   evaluation_periods  = 1
#   period              = 10
#   threshold           = 1
#   statistic           = "Sum"
#   comparison_operator = "GreaterThanOrEqualToThreshold"
#   namespace           = "pg8000LambdaErrorMetrics"
#   alarm_actions       = [aws_sns_topic.error_alerts.arn]
#   alarm_description   = "This alarm measures pg8000 error count"
# }

# resource "aws_cloudwatch_log_metric_filter" "invalid_DB_credentials" {
#   name           = "invalid DB credentials"
#   pattern        = "Parameter validation failed cant read database"
#   log_group_name = "/aws/lambda/${var.dummy_json_lambda}"
#   metric_transformation {
#     name      = "DBCredentialsErrorCount"
#     namespace = "DBCredentialsLambdaErrorMetrics"
#     value     = "1"
#   }
# }

# resource "aws_cloudwatch_metric_alarm" "invalid_DB_credentials_alarm" {
#   alarm_name          = "invalid_DB_credentials_error"
#   metric_name         = "DBCredentialsErrorCount"
#   evaluation_periods  = 1
#   period              = 10
#   threshold           = 1
#   statistic           = "Sum"
#   comparison_operator = "GreaterThanOrEqualToThreshold"
#   namespace           = "DBCredentialsLambdaErrorMetrics"
#   alarm_actions       = [aws_sns_topic.error_alerts.arn]
#   alarm_description   = "This alarm measures an invalid DB credentials error"
# }


