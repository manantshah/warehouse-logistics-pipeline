variable "snowflake_account" {
    type = string
    description = "The snowflake account identifier"
}
variable "snowflake_user" {
    type = string
    description = "The snowflake user"
}
variable "snowflake_password" {
    type = string
    sensitive = true
    description = "The snowflake password"
}