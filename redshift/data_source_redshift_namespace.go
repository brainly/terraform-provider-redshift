package redshift

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func dataSourceRedshiftNamespace() *schema.Resource {
	return &schema.Resource{
		Description: `Gets the cluster namespace (unique ID) of the Amazon Redshift cluster.`,
		ReadContext: RedshiftResourceFunc(dataSourceRedshiftNamespaceRead),
		Schema:      map[string]*schema.Schema{},
	}
}

func dataSourceRedshiftNamespaceRead(db *DBConnection, d *schema.ResourceData) error {
	var namespace string
	if err := db.QueryRow("SELECT CURRENT_NAMESPACE").Scan(&namespace); err != nil {
		return err
	}
	d.SetId(namespace)
	return nil
}
