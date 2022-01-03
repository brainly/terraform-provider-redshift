# Import user with usesysid: SELECT usesysid FROM pg_user_info WHERE usename = 'mememe'

terraform import redshift_user.mememe 123
