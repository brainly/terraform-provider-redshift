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
				ForceNew:     true,
				Description:  "The name of the role.",
				ValidateFunc: validation.StringMatch(regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]*$"), "Invalid role name format."),
			},
		},
	}
}

func resourceRedshiftRoleCreate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	roleName := d.Get(roleNameAttr).(string)
	sql := fmt.Sprintf("CREATE ROLE %s", pq.QuoteIdentifier(roleName))

	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("error creating Redshift role %s: %w", roleName, err)
	}

	// Use role name as the ID
	d.SetId(roleName)

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftRoleRead(db, d)
}

func resourceRedshiftRoleRead(db *DBConnection, d *schema.ResourceData) error {
	roleName := d.Id()

	var exists bool
	query := `
SELECT EXISTS (
	SELECT 1
	FROM svv_roles
	WHERE role_name = $1
)`

	err := db.QueryRow(query, roleName).Scan(&exists)
	switch {
	case err == sql.ErrNoRows || !exists:
		// Role not found, remove from state
		d.SetId("")
		return nil
	case err != nil:
		return fmt.Errorf("error reading Redshift role %s: %w", roleName, err)
	}

	d.Set(roleNameAttr, roleName)
	return nil
}

func resourceRedshiftRoleUpdate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer deferredRollback(tx)

	// Currently, only the name change is supported.
	if d.HasChange(roleNameAttr) {
		oldName, newName := d.GetChange(roleNameAttr)
		sql := fmt.Sprintf("ALTER ROLE %s RENAME TO %s", pq.QuoteIdentifier(oldName.(string)), pq.QuoteIdentifier(newName.(string)))

		if _, err := tx.Exec(sql); err != nil {
			return fmt.Errorf("error renaming Redshift role %s to %s: %w", oldName, newName, err)
		}

		// Update ID to the new role name for consistency
		d.SetId(newName.(string))
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftRoleRead(db, d)
}

func resourceRedshiftRoleDelete(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer deferredRollback(tx)

	roleName := d.Id()
	sql := fmt.Sprintf("DROP ROLE %s", pq.QuoteIdentifier(roleName))

	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("error deleting Redshift role %s: %w", roleName, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	// Remove the role from Terraform state
	d.SetId("")
	return nil
}

func resourceRedshiftRoleExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	roleName := d.Id()

	var exists bool
	query := `
SELECT EXISTS (
	SELECT 1
	FROM svv_roles
	WHERE role_name = $1
)`

	err := db.QueryRow(query, roleName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error checking existence of Redshift role %s: %w", roleName, err)
	}

	return exists, nil
}
