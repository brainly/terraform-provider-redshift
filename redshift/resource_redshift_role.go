package redshift

import (
	"database/sql"
	"fmt"
	"regexp"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	roleNameAttr = "name"
)

// redshiftRole defines the Terraform resource for Redshift roles.
func redshiftRole() *schema.Resource {
	return &schema.Resource{
		Description: "Manages Redshift roles, which allow assigning permissions to groups or users.",

		Create: RedshiftResourceFunc(resourceRedshiftRoleCreate),
		Read:   RedshiftResourceFunc(resourceRedshiftRoleRead),
		Update: RedshiftResourceFunc(resourceRedshiftRoleUpdate),
		Delete: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftRoleDelete),
		),
		Exists: RedshiftResourceExistsFunc(resourceRedshiftRoleExists),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			roleNameAttr: {
				Type:         schema.TypeString,
				Required:     true,
				Description:  "The name of the role.",
				ValidateFunc: validation.StringMatch(regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]*$"), "Invalid role name format."),
			},
		},
	}
}

// Create a new Redshift role
func resourceRedshiftRoleCreate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer deferredRollback(tx)

	roleName := d.Get(roleNameAttr).(string)
	sql := fmt.Sprintf("CREATE ROLE %s", pq.QuoteIdentifier(roleName))

	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("error creating Redshift role %s: %w", roleName, err)
	}

	var roleID string
	// Retrieve the role ID
	if err := tx.QueryRow("SELECT role_id FROM svv_roles WHERE role_name = $1", roleName).Scan(&roleID); err != nil {
		return fmt.Errorf("failed to retrieve role_id for role %q: %w", roleName, err)
	}
	d.SetId(roleID)

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftRoleRead(db, d)
}

// Read a Redshift role's information
func resourceRedshiftRoleRead(db *DBConnection, d *schema.ResourceData) error {
	roleID := d.Id()

	var roleName string
	query := `
SELECT role_name
FROM svv_roles
WHERE role_id = $1
`

	err := db.QueryRow(query, roleID).Scan(&roleName)
	switch {
	case err == sql.ErrNoRows:
		// Role not found, remove from state
		d.SetId("")
		return nil
	case err != nil:
		return fmt.Errorf("error reading Redshift role  %q: %w", roleName, err)
	}

	d.Set(roleNameAttr, roleName)
	return nil
}

// Update a Redshift role (currently supports renaming only)
func resourceRedshiftRoleUpdate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer deferredRollback(tx)

	if d.HasChange(roleNameAttr) {
		oldName, newName := d.GetChange(roleNameAttr)
		sql := fmt.Sprintf("ALTER ROLE %s RENAME TO %s", pq.QuoteIdentifier(oldName.(string)), pq.QuoteIdentifier(newName.(string)))

		if _, err := tx.Exec(sql); err != nil {
			return fmt.Errorf("error renaming Redshift role from %q to %q: %w", oldName, newName, err)
		}

		// Update the role name in the state
		d.Set(roleNameAttr, newName)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftRoleRead(db, d)
}

// Delete a Redshift role
// Delete a Redshift role
func resourceRedshiftRoleDelete(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer deferredRollback(tx)

	// Get the role name from Terraform state
	roleName := d.Get(roleNameAttr).(string)

	// Execute DROP ROLE SQL statement
	sql := fmt.Sprintf("DROP ROLE %s", pq.QuoteIdentifier(roleName))
	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("error deleting Redshift role %q: %w", roleName, err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	// Remove the role from Terraform state
	d.SetId("")
	return nil
}

// Check if a Redshift role exists
func resourceRedshiftRoleExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	roleID := d.Id()

	var exists bool
	query := `
SELECT EXISTS (
	SELECT 1
	FROM svv_roles
	WHERE role_id = $1
)
`

	err := db.QueryRow(query, roleID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error checking existence of Redshift role with ID %q: %w", roleID, err)
	}

	return exists, nil
}
