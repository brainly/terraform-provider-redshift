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

# Granting permission to PUBLIC (GRANT ... TO PUBLIC)
resource "redshift_grant" "public" {
  group       = "public" // "public" or "PUBLIC" (it is case insensitive for this case) here indicates we want grant TO PUBLIC, not "public" group which cannot even be created in Redshift (keyword).
  schema      = "my_schema"
  object_type = "schema"
  privileges  = ["usage"]
}
