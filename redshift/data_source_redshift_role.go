package redshift

import (
	"database/sql"
	"fmt"
	"regexp"

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

	var exists bool
	query := `
SELECT EXISTS (
	SELECT 1
	FROM svv_roles
	WHERE role_name = $1
)`

	// Check if the role exists
	err := db.QueryRow(query, roleName).Scan(&exists)
	switch {
	case err == sql.ErrNoRows || !exists:
		return fmt.Errorf("Redshift role %s not found", roleName)
	case err != nil:
		return fmt.Errorf("error checking Redshift role %s: %w", roleName, err)
	}

	// Use role name as the ID for the data source
	d.SetId(roleName)

	// Set attributes
	d.Set(roleNameAttr, roleName)

	return nil
}
