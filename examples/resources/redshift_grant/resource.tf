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

# Granting permissions to execute functions or procedures requires providing their arguments' types

resource "redshift_grant" "user" {
  user        = "john"
  schema      = "my_schema"
  object_type = "function"
  objects     = ["my_function(float)"]
  privileges  = ["execute"]
}

resource "redshift_grant" "datashare_database" {
  group       = "analysts"
  object_type = "datashare_database"
  objects     = ["analysts_datashare_database"]
  privileges  = ["usage"]
}
