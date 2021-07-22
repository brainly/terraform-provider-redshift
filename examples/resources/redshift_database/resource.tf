# Example resource declaration of a local database
resource "redshift_database" "db" {
  name = "my_database"
  owner = "my_user"
  connection_limit = 123456 # use -1 for unlimited

  lifecycle {
    prevent_destroy = true
  }
}


# Example resource declaration of a database
# created from a datashare of another redshift cluster
resource "redshift_database" "datashare_db" {
  name = "my_datashare_consumer_db"
  owner = "my_user"
  connection_limit = 123456 # use -1 for unlimited

  datashare_source {
    share_name = "my_datashare"
    account_id = "123456789012" # 12 digit AWS account number of the producer cluster (optional, default is current account)
    namespace = "00000000-0000-0000-0000-000000000000" # producer cluster namespace (uuid)
  }
}
