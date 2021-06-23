resource "redshift_group" "staff" {
  name = "group_users"
  users = [
    redshift_user.user.name,
    redshift_user.other.name,
  ]
}
