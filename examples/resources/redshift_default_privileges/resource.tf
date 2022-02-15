resource "redshift_default_privileges" "group" {
  group       = "analysts"
  owner       = "root"
  object_type = "table"
  privileges  = ["select"]
}

resource "redshift_default_privileges" "user" {
  user        = "john"
  owner       = "root"
  object_type = "table"
  privileges  = ["select", "update", "insert", "delete", "drop", "references"]
}
