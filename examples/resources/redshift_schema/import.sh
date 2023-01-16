# Import schema with oid: SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'myschema';

terraform import redshift_schema.myschema 234
