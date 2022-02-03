provider "redshift" {
  host     = var.redshift_host
  username = var.redshift_user
  temporary_credentials {
    cluster_identifier = "my-cluster"
    assume_role {
      arn = "arn:aws:iam::012345678901:role/role-name-with-path"
    }
  }
}
