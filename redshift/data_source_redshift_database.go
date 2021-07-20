package redshift

import (
	"database/sql"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

const (
	datashareProducerNameAttr      = "datashare_producer_name"
	datashareProducerAccountAttr   = "datashare_producer_account"
	datashareProducerNamespaceAttr = "datashare_producer_namespace"
)

func dataSourceRedshiftDatabase() *schema.Resource {
	return &schema.Resource{
		Description: `Fetches information about a Redshift database.`,
		Read:        RedshiftResourceFunc(dataSourceRedshiftDatabaseRead),
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
			datashareProducerNameAttr: {
				Type:        schema.TypeString,
				Computed:    true,
				Optional:    true,
				Description: "For databases created from datashares, this is the producer's datashare name.",
			},
			datashareProducerAccountAttr: {
				Type:        schema.TypeString,
				Computed:    true,
				Optional:    true,
				Description: "For databases created from datashares, this is the producer's account number.",
			},
			datashareProducerNamespaceAttr: {
				Type:        schema.TypeString,
				Computed:    true,
				Optional:    true,
				Description: "For databases created from datashares, this is the producer's namespace.",
			},
		},
	}
}

func dataSourceRedshiftDatabaseRead(db *DBConnection, d *schema.ResourceData) error {
	var id, owner, connLimit string
	var shareName, producerAccount, producerNamespace sql.NullString

	err := db.QueryRow(`SELECT
  pg_database_info.datid,
  trim(pg_user_info.usename),
  COALESCE(pg_database_info.datconnlimit::text, 'UNLIMITED'),
  trim(svv_datashares.share_name),
  trim(svv_datashares.producer_account),
  trim(svv_datashares.producer_namespace)
FROM
  svv_redshift_databases
LEFT JOIN pg_database_info
  ON svv_redshift_databases.database_name=pg_database_info.datname
LEFT JOIN pg_user_info
  ON pg_user_info.usesysid = svv_redshift_databases.database_owner
LEFT JOIN svv_datashares
	on (svv_datashares.share_type='INBOUND' AND svv_datashares.consumer_database=svv_redshift_databases.database_name)
WHERE svv_redshift_databases.database_name = $1
	`, d.Get(databaseNameAttr).(string)).Scan(&id, &owner, &connLimit, &shareName, &producerAccount, &producerNamespace)

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

	if shareName.Valid {
		d.Set(datashareProducerNameAttr, shareName.String)
	} else {
		d.Set(datashareProducerNameAttr, nil)
	}

	if producerAccount.Valid {
		d.Set(datashareProducerAccountAttr, producerAccount.String)
	} else {
		d.Set(datashareProducerAccountAttr, nil)
	}

	if producerNamespace.Valid {
		d.Set(datashareProducerNamespaceAttr, producerNamespace.String)
	} else {
		d.Set(datashareProducerNamespaceAttr, nil)
	}

	return nil
}
