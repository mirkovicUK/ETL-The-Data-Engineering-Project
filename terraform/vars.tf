variable "parquet_lambda_name" {
    type = string
    default = "json_to_parquet_lambda"
}

variable "json_to_parquet_lambda" {
    type = string
    default = "parquet_to_json_lambda"
}

variable "dummy_json_lambda" {
    type = string
    default = "dummy_lambda"
}