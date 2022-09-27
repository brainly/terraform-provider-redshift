# Import group with grosysid: SELECT grosysid FROM pg_group WHERE groname = 'mygroup'

terraform import redshift_group_membership.mygroup 234
