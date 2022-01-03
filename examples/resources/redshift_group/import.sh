# Import group with grosysid: SELECT grosysid FROM pg_group WHERE groname = 'mygroup'

terraform import redshift_group.mygroup 234
