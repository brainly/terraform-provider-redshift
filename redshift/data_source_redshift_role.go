package redshift

import (
	"database/sql"
	"fmt"
	"regexp"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

// dataSourceRedshiftRole defines the Redshift role data source.
func dataSourceRedshiftRole() *schema.Resource {
	return &schema.Resource{
		Description: "Retrieves information about a Redshift role.",

		Read: RedshiftResourceFunc(dataSourceRedshiftRoleRead),

		Schema: map[string]*schema.Schema{
			roleNameAttr: {
				Type:         schema.TypeString,
				Required:     true,
				Description:  "The name of the Redshift role.",
				ValidateFunc: validation.StringMatch(regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]*$"), "Invalid role name format."),
			},
		},
	}
}

func dataSourceRedshiftRoleRead(db *DBConnection, d *schema.ResourceData) error {
	roleName := d.Get(roleNameAttr).(string)

	var roleID string
	query := `
SELECT role_id
FROM svv_roles
WHERE role_name = $1
`

	retries := 5
	var err error
	for i := 0; i < retries; i++ {
		err = db.QueryRow(query, roleName).Scan(&roleID)
		if err == nil {
			break
		}
		if err == sql.ErrNoRows && i < retries-1 {
			time.Sleep(2 * time.Second) // Retry after delay if role isn't found
			continue
		}
		return fmt.Errorf("failed to retrieve Redshift role %q: %w", roleName, err)
	}

	// Use role_id as the ID for the data source
	d.SetId(roleID)

	// Set attributes
	d.Set(roleNameAttr, roleName)

	return nil
}
