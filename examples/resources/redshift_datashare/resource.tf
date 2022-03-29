resource "redshift_datashare" "my_datashare" {
  name = "my_datashare" # Required
  owner = "my_user" # Optional.
  publicly_accessible = false # Optional. Default is `false`.

  # Optional. Specifies which schemas to expose to the datashare.
  schemas = [
    "public",
    "other",
  ]
  # Optional. Specifies which schema tables to expose to the datashare.
  schema_tables = [
    "schema1.table1",
    "schema1.table2",
    "schema2.table1",
    "schema2.table2",
  ]
}
