resource "redshift_group_membership" "staff_group_membership" {
  group_name = "group_users"
  users = [
    redshift_user.user.name,
    redshift_user.other.name,
  ]
}
