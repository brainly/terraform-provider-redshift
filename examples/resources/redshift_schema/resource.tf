resource "redshift_user" "owner" {
  name = "owner"
}

resource "redshift_schema" "schema" {
  name  = "my_schema"
  owner = redshift_user.owner.name
  quota = 150
}
