# Example: data share which includes all tables/views and functions in the specified schema.
# New tables/views and functions are automatically added to the datashare.
resource "redshift_datashare" "datashare_auto" {
  name = "my_automatic_datashare" # Required
  owner = "my_user" # Optional

  schema {
    name = "public" # Required
    mode = "auto" # Required
  }
}

# Example: data share which explicitly specifies tables/views and functions
resource "redshift_datashare" "datashare_manual" {
  name = "my_manual_datashare" # Required

  schema {
    name = "public" # Required
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

  schema {
    name = "public" # Required
    mode = "auto" # Required
  }
}
