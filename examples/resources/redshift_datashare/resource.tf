resource "redshift_datashare" "my_datashare" {
  name = "my_datashare" # Required
  owner = "my_user" # Optional.
  publicly_accessible = false # Optional. Default is `false`.

  # Optional. Specifies which schemas to expose to the datashare.
  schemas = [
    "public",
    "other",
  ]
}
