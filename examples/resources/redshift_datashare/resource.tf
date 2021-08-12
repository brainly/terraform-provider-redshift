# Example: Datashare that can only be consumed by a non-public Redshift cluster.
resource "redshift_datashare" "private_datashare" {
  name = "my_private_datashare" # Required
  owner = "my_user" # Optional

  # Example of adding a schema to a data share in "auto" mode.
  # All tables/views and functions in the schema are added to the datashare,
  # and redshift will automatically add newly-created tables/views and functions
  # to the datashare without needing to re-run terraform.
  schema {
    name = "public" # Required
    mode = "auto" # Required
  }

  # Example of ading a schema to a data share in "manual" mode.
  # Only the specified tables/views and functions will be added to the data share.
  schema {
    name = "other" # Required
    mode = "manual" # Required
    tables = [ # Optional. If unspecified then no tables/views will be added.
      "my_table",
      "my_view",
      "my_late_binding_view",
      "my_materialized_view",
    ]
    functions = [ # Optional. If unspecified then no functions will be added.
      "my_sql_udf",
    ]
  }
}

# Example: Datashare that can be shared with publicly available consumer clusters.
resource "redshift_datashare" "publicly_accessible_datashare" {
  name = "my_public_datashare" # Required
  publicly_accessible = true # Optional. Default is `false`

  # Example of adding a schema to a data share in "auto" mode.
  # All tables/views and functions in the schema are added to the datashare,
  # and redshift will automatically add newly-created tables/views and functions
  # to the datashare without needing to re-run terraform.
  schema {
    name = "public" # Required
    mode = "auto" # Required
  }

  # Example of ading a schema to a data share in "manual" mode.
  # Only the specified tables/views and functions will be added to the data share.
  schema {
    name = "other" # Required
    mode = "manual" # Required
    tables = [ # Optional. If unspecified then no tables/views will be added.
      "my_table",
      "my_view",
      "my_late_binding_view",
      "my_materialized_view",
    ]
    functions = [ # Optional. If unspecified then no functions will be added.
      "my_sql_udf",
    ]
  }
}
