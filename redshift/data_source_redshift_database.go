package redshift

import (
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func dataSourceRedshiftDatabase() *schema.Resource {
	return &schema.Resource{
		Description: `Fetches information about a Redshift database.`,
		ReadContext: RedshiftResourceFunc(dataSourceRedshiftDatabaseRead),
		Schema: map[string]*schema.Schema{
			databaseNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of the database",
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			databaseOwnerAttr: {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Owner of the database, usually the user who created it",
			},
			databaseConnLimitAttr: {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "The maximum number of concurrent connections that can be made to this database. A value of -1 means no limit.",
			},
			databaseDatashareSourceAttr: {
				Type:        schema.TypeList,
				Optional:    true,
				MaxItems:    1,
				Description: "Configuration for a database created from a redshift datashare.",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						databaseDatashareSourceShareNameAttr: {
							Type:        schema.TypeString,
							Optional:    true,
							Computed:    true,
							Description: "The name of the datashare on the producer cluster",
							StateFunc: func(val interface{}) string {
								return strings.ToLower(val.(string))
							},
						},
						databaseDatashareSourceNamespaceAttr: {
							Type:        schema.TypeString,
							Optional:    true,
							Computed:    true,
							Description: "The namespace (guid) of the producer cluster",
							StateFunc: func(val interface{}) string {
								return strings.ToLower(val.(string))
							},
						},
						databaseDatashareSourceAccountAttr: {
							Type:        schema.TypeString,
							Optional:    true,
							Computed:    true,
							Description: "The AWS account ID of the producer cluster.",
						},
					},
				},
			},
		},
	}
}

func dataSourceRedshiftDatabaseRead(db *DBConnection, d *schema.ResourceData) error {
	var id, owner, connLimit, databaseType, shareName, producerAccount, producerNamespace string

	err := db.QueryRow(`SELECT
  pg_database_info.datid,
  TRIM(pg_user_info.usename),
  COALESCE(pg_database_info.datconnlimit::text, 'UNLIMITED'),
	svv_redshift_databases.database_type,
  TRIM(COALESCE(svv_datashares.share_name, '')),
  TRIM(COALESCE(svv_datashares.producer_account, '')),
  TRIM(COALESCE(svv_datashares.producer_namespace, ''))
FROM
  svv_redshift_databases
LEFT JOIN pg_database_info
  ON svv_redshift_databases.database_name=pg_database_info.datname
LEFT JOIN pg_user_info
  ON pg_user_info.usesysid = svv_redshift_databases.database_owner
LEFT JOIN svv_datashares
	ON (svv_redshift_databases.database_name = svv_datashares.consumer_database AND svv_redshift_databases.database_type = 'shared' AND svv_datashares.share_type = 'INBOUND')
WHERE svv_redshift_databases.database_name = $1
	`, d.Get(databaseNameAttr).(string)).Scan(&id, &owner, &connLimit, &databaseType, &shareName, &producerAccount, &producerNamespace)

	if err != nil {
		return err
	}

	connLimitNumber := -1
	if connLimit != "UNLIMITED" {
		if connLimitNumber, err = strconv.Atoi(connLimit); err != nil {
			return err
		}
	}

	d.SetId(id)
	d.Set(databaseOwnerAttr, owner)
	d.Set(databaseConnLimitAttr, connLimitNumber)

	dataShareConfiguration := make([]map[string]interface{}, 0, 1)
	if databaseType == "shared" {
		config := make(map[string]interface{})
		config[databaseDatashareSourceShareNameAttr] = &shareName
		config[databaseDatashareSourceAccountAttr] = &producerAccount
		config[databaseDatashareSourceNamespaceAttr] = &producerNamespace
		dataShareConfiguration = append(dataShareConfiguration, config)
	}
	d.Set(databaseDatashareSourceAttr, dataShareConfiguration)

	return nil
}
