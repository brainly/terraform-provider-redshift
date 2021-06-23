resource "redshift_user" "user" {
  name      = "UserName"
  password  = "secret password"
  superuser = true
}

resource "redshift_user" "user_with_unrestricted_syslog" {
  name          = "user_syslog"
  syslog_access = "UNRESTRICTED"
}
