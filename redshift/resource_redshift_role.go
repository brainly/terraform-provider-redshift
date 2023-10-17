package redshift

import (
	"database/sql"
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	roleNameAttr = "name"
)

func redshiftRole() *schema.Resource {
	return &schema.Resource{
		Description: `
Roles are collections of permissions that you can assign to a user or another role. You can assign system or database permissions to a role. A user inherits permissions from an assigned role.
`,
		Create: RedshiftResourceFunc(resourceRedshiftRoleCreate),
		Read:   RedshiftResourceFunc(resourceRedshiftRoleRead),
		Update: RedshiftResourceFunc(resourceRedshiftRoleUpdate),
		Delete: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftRoleDelete),
		),
		Exists: RedshiftResourceExistsFunc(resourceRedshiftRoleExists),
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			roleNameAttr: {
				Type:         schema.TypeString,
				Required:     true,
				Description:  "The name of the role (case sensitive). The role name must be unique and can't be the same as any user names. A role name can't be a reserved word.",
				ValidateFunc: validation.StringNotInSlice(reservedWords, false),
			},
		},
	}
}

func resourceRedshiftRoleExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	var roleName string
	err := db.QueryRow("SELECT role_name FROM svv_roles WHERE role_id = $1", d.Id()).Scan(&roleName)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func resourceRedshiftRoleRead(db *DBConnection, d *schema.ResourceData) error {
	return resourceRedshiftRoleReadImpl(db, d)
}

func resourceRedshiftRoleReadImpl(db *DBConnection, d *schema.ResourceData) error {
	var (
		roleName string
	)

	sql := `SELECT role_name from svv_roles WHERE role_id=$1`
	if err := db.QueryRow(sql, d.Id()).Scan(&roleName); err != nil {
		return err
	}

	d.Set(roleNameAttr, roleName)

	return nil
}

func resourceRedshiftRoleCreate(db *DBConnection, d *schema.ResourceData) error {
	roleName := d.Get(roleNameAttr).(string)

	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	create_role_sql := fmt.Sprintf("CREATE ROLE %s", pq.QuoteIdentifier(roleName))
	if _, err := tx.Exec(create_role_sql); err != nil {
		return fmt.Errorf("Could not create redshift role: %s", err)
	}

	var roleId string
	find_role_id_sql := fmt.Sprintf("SELECT role_id FROM svv_roles WHERE role_name = %s", pq.QuoteLiteral(roleName))
	if err := tx.QueryRow(find_role_id_sql).Scan(&roleId); err != nil {
		return fmt.Errorf("Could not get redshift role id for '%s': %s", roleName, err)
	}

	d.SetId(roleId)

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftRoleReadImpl(db, d)
}

func resourceRedshiftRoleDelete(db *DBConnection, d *schema.ResourceData) error {
	roleName := d.Get(roleNameAttr).(string)

	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if _, err := tx.Exec(fmt.Sprintf("DROP ROLE %s", pq.QuoteIdentifier(roleName))); err != nil {
		return err
	}

	return tx.Commit()
}

func resourceRedshiftRoleUpdate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := setRoleName(tx, d); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftRoleReadImpl(db, d)
}

func setRoleName(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(roleNameAttr) {
		return nil
	}

	oldRaw, newRaw := d.GetChange(roleNameAttr)
	oldValue := oldRaw.(string)
	newValue := newRaw.(string)

	if newValue == "" {
		return fmt.Errorf("Error setting role name to an empty string")
	}

	sql := fmt.Sprintf("ALTER ROLE %s RENAME TO %s", pq.QuoteIdentifier(oldValue), pq.QuoteIdentifier(newValue))
	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("Error updating Role NAME: %w", err)
	}

	return nil
}
