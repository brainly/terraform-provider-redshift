provider "redshift" {
  host     = var.redshift_host
  username = var.redshift_user
  temporary_credentials {
    cluster_identifier = "my-cluster"
  }
}
