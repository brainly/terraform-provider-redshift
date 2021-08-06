resource "redshift_user" "owner" {
  name = "owner"
}

# Internal schema
resource "redshift_schema" "schema" {
  name  = "my_schema"
  owner = redshift_user.owner.name
  quota = 150
}

# External schema using AWS Glue Data Catalog
resource "redshift_schema" "external_from_glue_data_catalog" {
  name = "spectrum_schema"
  owner = redshift_user.owner.name
  external_schema {
    database_name = "spectrum_db" # Required. Name of the db in glue catalog
    data_catalog_source {
      region = "us-west-2" # Optional. If not specified, Redshift will use the same region as the cluster.
      iam_role_arns = [
        # Required. Must be at least 1 ARN and not more than 10.
        "arn:aws:iam::123456789012:role/myRedshiftRole",
        "arn:aws:iam::123456789012:role/myS3Role",
      ]
      catalog_role_arns = [
        # Optional. If specified, must be at least 1 ARN and not more than 10.
        # If not specified, Redshift will use iam_role_arns for accessing the glue data catalog.
        "arn:aws:iam::123456789012:role/myAthenaRole",
        # ...
      ]
      create_external_database_if_not_exists = true # Optional. Defaults to false.
    }
  }
}

# External schema using Hive Metastore
resource "redshift_schema" "external_from_hive_metastore" {
  name = "hive_schema"
  owner = redshift_user.owner.name
  external_schema {
    database_name = "hive_db" # Required. Name of the db in hive metastore
    hive_metastore_source {
      hostname = "172.10.10.10" # Required
      port = 99 # Optional. Default is 9083
      iam_role_arns = [
        # Required. Must be at least 1 ARN and not more than 10.
        "arn:aws:iam::123456789012:role/MySpectrumRole",
      ]
    }
  }
}

# External schema using federated query from RDS/Aurora Postgres
resource "redshift_schema" "external_from_postgres" {
  name = "myRedshiftPostgresSchema"
  owner = redshift_user.owner.name
  external_schema {
    database_name = "my_aurora_db" # Required. Name of the db in postgres
    rds_postgres_source {
      hostname = "endpoint to aurora hostname" # Required
      port = 5432 # Optional. Default is 5432
      schema = "my_aurora_schema" # Optional, default is "public"
      iam_role_arns = [
        # Required. Must be at least 1 ARN and not more than 10.
        "arn:aws:iam::123456789012:role/MyAuroraRole",
        # ...
      ]
      secret_arn = "arn:aws:secretsmanager:us-east-2:123456789012:secret:development/MyTestDatabase-AbCdEf" # Required
    }
  }
}

# External schema using federated query from RDS/Aurora MySQL
resource "redshift_schema" "external_from_mysql" {
  name = "myRedshiftMysqlSchema"
  owner = redshift_user.owner.name
  external_schema {
    database_name = "my_aurora_db" # Required. Name of the db in mysql
    rds_mysql_source {
      hostname = "endpoint to aurora hostname" # Required
      port = 3306 # Optional. Default is 3306
      iam_role_arns = [
        # Required. Must be at least 1 ARN and not more than 10.
        "arn:aws:iam::123456789012:role/MyAuroraRole",
        # ...
      ]
      secret_arn = "arn:aws:secretsmanager:us-east-2:123456789012:secret:development/MyTestDatabase-AbCdEf" # Required
    }
  }
}

# External schema using federated query from Redshift data share database
resource "redshift" "external_from_redshift" {
  name = "Sales_schema"
  owner = redshift_user.owner.name
  external_schema {
    database_name = "Sales_db" # Required. Name of the datashare db
    redshift_source {
      schema = "public" # Optional. Name of the schema in the datashare db. Default is "public"
    }
  }
}
