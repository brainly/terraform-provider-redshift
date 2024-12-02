package redshift

import (
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func dataSourceRedshiftRoleGrant() *schema.Resource {
	return &schema.Resource{
		Read: RedshiftResourceFunc(dataSourceRedshiftRoleGrantRead),

		Schema: map[string]*schema.Schema{
			"user": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"role"},
				Description:   "The name of the user for which to retrieve granted roles.",
			},
			"role": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"user"},
				Description:   "The name of the role for which to retrieve granted roles.",
			},
			"granted_roles": {
				Type:        schema.TypeList,
				Computed:    true,
				Description: "A list of roles granted to the specified user or role.",
				Elem:        &schema.Schema{Type: schema.TypeString},
			},
		},
	}
}

func dataSourceRedshiftRoleGrantRead(db *DBConnection, d *schema.ResourceData) error {
	// Determine whether we're querying for a user or a role
	user := d.Get("user").(string)
	role := d.Get("role").(string)

	var query string
	var queryArgs []interface{}
	var entityName string

	if user != "" {
		query = `
			SELECT role_name
			FROM svv_user_grants
			WHERE user_name = $1
		`
		queryArgs = []interface{}{user}
		entityName = user
	} else if role != "" {
		query = `
			SELECT granted_role_name
			FROM svv_role_grants
			WHERE role_name = $1
		`
		queryArgs = []interface{}{role}
		entityName = role
	} else {
		return fmt.Errorf("either 'user' or 'role' must be specified")
	}

	// Execute the query
	rows, err := db.Query(query, queryArgs...)
	if err != nil {
		return fmt.Errorf("error querying role grants: %w", err)
	}
	defer rows.Close()

	grantedRoles := []string{}
	for rows.Next() {
		var grantedRole string
		if err := rows.Scan(&grantedRole); err != nil {
			return fmt.Errorf("error scanning role grant row: %w", err)
		}
		grantedRoles = append(grantedRoles, grantedRole)
	}

	// Generate a unique ID for the data source
	id := strings.Join([]string{entityName, "grants"}, "-")

	d.SetId(id)
	d.Set("granted_roles", grantedRoles)

	log.Printf("[DEBUG] Retrieved grants for %s: %v", entityName, grantedRoles)

	return nil
}
