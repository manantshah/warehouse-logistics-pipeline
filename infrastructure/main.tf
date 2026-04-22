terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87"
    }
  }
}

provider "snowflake" {
  account  = var.snowflake_account
  user     = var.snowflake_user
  password = var.snowflake_password
  role     = "ACCOUNTADMIN"
}

# 1. Create the Role & Grant it to you
resource "snowflake_role" "pipeline_role" {
  name = "PIPELINE_ROLE"
}

resource "snowflake_grant_account_role" "grant_to_user" {
  role_name = snowflake_role.pipeline_role.name
  user_name = upper(var.snowflake_user)
}

# 2. Create the Compute Warehouse
resource "snowflake_warehouse" "fulfillment_wh" {
  name           = "FULFILLMENT_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
}

# 3. Create the Database & Schemas
resource "snowflake_database" "fulfillment_db" {
  name = "FULFILLMENT_DB"
}

resource "snowflake_schema" "raw_schema" {
  database = snowflake_database.fulfillment_db.name
  name     = "RAW"
}

resource "snowflake_schema" "analytics_schema" {
  database = snowflake_database.fulfillment_db.name
  name     = "ANALYTICS"
}

# 4. Wire up Permissions
resource "snowflake_grant_privileges_to_account_role" "wh_grant" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_role.pipeline_role.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.fulfillment_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "db_grant" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_role.pipeline_role.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.fulfillment_db.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_grant" {
  privileges        = ["ALL PRIVILEGES"]
  account_role_name = snowflake_role.pipeline_role.name
  on_schema {
    schema_name = "\"${snowflake_database.fulfillment_db.name}\".\"${snowflake_schema.raw_schema.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "analytics_schema_grant" {
  privileges        = ["ALL PRIVILEGES"]
  account_role_name = snowflake_role.pipeline_role.name
  on_schema {
    schema_name = "\"${snowflake_database.fulfillment_db.name}\".\"${snowflake_schema.analytics_schema.name}\""
  }
}