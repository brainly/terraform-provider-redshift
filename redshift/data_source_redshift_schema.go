package redshift

import (
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func dataSourceRedshiftSchema() *schema.Resource {
	return &schema.Resource{
		Description: `
A database contains one or more named schemas. Each schema in a database contains tables and other kinds of named objects. By default, a database has a single schema, which is named PUBLIC. You can use schemas to group database objects under a common name. Schemas are similar to file system directories, except that schemas cannot be nested.
`,
		Read: RedshiftResourceFunc(dataSourceRedshiftSchemaRead),
		Schema: map[string]*schema.Schema{
			schemaNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of the schema.",
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			schemaOwnerAttr: {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Name of the schema owner.",
			},
			schemaQuotaAttr: {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "The maximum amount of disk space that the specified schema can use. GB is the default unit of measurement.",
			},
		},
	}
}

func dataSourceRedshiftSchemaRead(db *DBConnection, d *schema.ResourceData) error {
	var schemaOwner, schemaId string
	var schemaQuota int

	err := db.QueryRow(`
			SELECT
			  pg_namespace.oid,
			  trim(usename),
			  COALESCE(quota, 0)
			FROM pg_namespace
			  LEFT JOIN svv_schema_quota_state
			    ON svv_schema_quota_state.schema_id = pg_namespace.oid
			  LEFT JOIN pg_user_info
			    ON pg_user_info.usesysid = pg_namespace.nspowner
			WHERE pg_namespace.nspname = $1`, d.Get(schemaNameAttr).(string)).Scan(&schemaId, &schemaOwner, &schemaQuota)

	if err != nil {
		return err
	}

	d.SetId(schemaId)
	d.Set(schemaOwnerAttr, schemaOwner)
	d.Set(schemaQuotaAttr, schemaQuota)

	return nil
}
