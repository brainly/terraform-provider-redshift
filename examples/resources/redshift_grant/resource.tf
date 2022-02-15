resource "redshift_grant" "user" {
  user        = "john"
  schema      = "my_schema"
  object_type = "schema"
  privileges  = ["create", "usage"]
}

resource "redshift_grant" "group" {
  group       = "analysts"
  schema      = "my_schema"
  object_type = "schema"
  privileges  = ["usage"]
}
