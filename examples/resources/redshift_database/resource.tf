resource "redshift_database" "db" {
  name = "my_database"
  owner = "my_user"
  connection_limit = 123456 # use -1 for unlimited

  lifecycle {
    prevent_destroy = true
  }
}
