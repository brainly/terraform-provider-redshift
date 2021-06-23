resource "redshift_privilege" "readonly_tables" {
  group       = "test_group"
  schema      = "public"
  object_type = "table"
  privileges  = ["SELECT"]
}

resource "redshift_privilege" "revoke_public" {
  group       = "public"
  schema      = "public"
  object_type = "schema"
  privileges  = []
}
