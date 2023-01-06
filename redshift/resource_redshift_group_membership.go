package redshift

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	groupMembershipNameAttr  = "group_name"
	groupMembershipUsersAttr = "users"
)

func redshiftGroupMembership() *schema.Resource {
	return &schema.Resource{
		Description: `
Group Membership defines a collection of users who are associated with the group. You can use this resource to manage group membership separate from the group resource. If you are using it in conjugation with redshift_group resource, please don't forget to use ignore_changes meta-argument with the users field in the redshift_group resource. Otherwise any changes done by redshift_group_membership resource will be overriden by redshift_group resource.
`,
		Create: RedshiftResourceFunc(resourceRedshiftGroupMembershipCreate),
		Read:   RedshiftResourceFunc(resourceRedshiftGroupMembershipRead),
		Update: RedshiftResourceFunc(resourceRedshiftGroupMembershipUpdate),
		Delete: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftGroupMembershipDelete),
		),
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			groupMembershipNameAttr: {
				Type:         schema.TypeString,
				Required:     true,
				Description:  "Name of the user group. Group names beginning with two underscores are reserved for Amazon Redshift internal use.",
				ValidateFunc: validation.StringDoesNotMatch(regexp.MustCompile("^__.*"), "Group names beginning with two underscores are reserved for Amazon Redshift internal use"),
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			groupMembershipUsersAttr: {
				Type:     schema.TypeSet,
				Required: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Description: "List of the user names to add to the group",
			},
		},
	}
}

func resourceRedshiftGroupMembershipRead(db *DBConnection, d *schema.ResourceData) error {
	return resourceRedshiftGroupMembershipReadImpl(db, d)
}

func resourceRedshiftGroupMembershipReadImpl(db *DBConnection, d *schema.ResourceData) error {
	var (
		groupName  string
		groupUsers []string
	)

	sql := `SELECT ARRAY(SELECT u.usename FROM pg_user_info u, pg_group g WHERE g.grosysid = $1 AND u.usesysid = ANY(g.grolist)) AS members, groname FROM pg_group WHERE grosysid = $1`
	if err := db.QueryRow(sql, d.Id()).Scan(pq.Array(&groupUsers), &groupName); err != nil {
		if strings.Contains(err.Error(), "no rows in result set") {
			d.SetId("")
		} else {
			sql = fmt.Sprintf("%s : $1=%s", sql, d.Id())
			return fmt.Errorf("Error running SQL query (%s): %w", sql, err)
		}
	} else {
		d.Set(groupMembershipNameAttr, groupName)
		d.Set(groupMembershipUsersAttr, groupUsers)
	}

	return nil
}

func resourceRedshiftGroupMembershipUpdate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := updateMemberUsersNames(tx, db, d); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftGroupMembershipReadImpl(db, d)
}

func resourceRedshiftGroupMembershipCreate(db *DBConnection, d *schema.ResourceData) error {
	groupName := d.Get(groupMembershipNameAttr).(string)
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	groupExists, err := checkIfGroupExists(tx, groupName)
	if err != nil {
		return err
	}

	if groupExists {
		var groSysID string
		if err := tx.QueryRow("SELECT grosysid FROM pg_group WHERE groname = $1", strings.ToLower(groupName)).Scan(&groSysID); err != nil {
			return fmt.Errorf("Could not get redshift group id for '%s': %s", groupName, err)
		}
		d.SetId(groSysID)

		if err := addMemberUsersNames(tx, db, d); err != nil {
			return err
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("could not commit transaction: %w", err)
		}

		return resourceRedshiftGroupMembershipReadImpl(db, d)

	} else {
		return fmt.Errorf("Group %s doesn't exist", groupName)
	}

}

func resourceRedshiftGroupMembershipDelete(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := removeMemberUsersNames(tx, db, d); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftGroupMembershipReadImpl(db, d)

}

func setMemberGroupName(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(groupMembershipNameAttr) {
		return nil
	}

	oldRaw, newRaw := d.GetChange(groupMembershipNameAttr)
	oldValue := oldRaw.(string)
	newValue := newRaw.(string)

	if newValue == "" {
		return fmt.Errorf("Error setting group name to an empty string")
	}

	sql := fmt.Sprintf("ALTER GROUP %s RENAME TO %s", pq.QuoteIdentifier(oldValue), pq.QuoteIdentifier(newValue))
	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("Error updating Group NAME: %w", err)
	}

	return nil
}

func checkIfMemberUserExists(tx *sql.Tx, name string) (bool, error) {

	var result int
	err := tx.QueryRow("SELECT 1 from pg_user_info WHERE usename=$1", name).Scan(&result)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("error reading info about user: %s", err)
	}

	return true, nil
}
func checkIfGroupExists(tx *sql.Tx, name string) (bool, error) {

	var result int
	err := tx.QueryRow("SELECT 1 from pg_group WHERE groname=$1", name).Scan(&result)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("error reading info about group: %s", err)
	}

	return true, nil
}

func updateMemberUsersNames(tx *sql.Tx, db *DBConnection, d *schema.ResourceData) error {
	if !d.HasChange(groupMembershipUsersAttr) {
		return nil
	}

	groupName := d.Get(groupMembershipNameAttr).(string)
	oldUsersSet, newUsersSet := d.GetChange(groupMembershipUsersAttr)
	removedUsers := oldUsersSet.(*schema.Set).Difference(newUsersSet.(*schema.Set))
	addedUsers := newUsersSet.(*schema.Set).Difference(oldUsersSet.(*schema.Set))

	if removedUsers.Len() > 0 {
		removedUsersNamesSafe := []string{}
		for _, name := range removedUsers.List() {
			userExists, err := checkIfMemberUserExists(tx, name.(string))
			if err != nil {
				return err
			}

			if userExists {
				removedUsersNamesSafe = append(removedUsersNamesSafe, pq.QuoteIdentifier(name.(string)))
			}
		}

		if len(removedUsersNamesSafe) > 0 {
			sql := fmt.Sprintf("ALTER GROUP %s DROP USER %s", pq.QuoteIdentifier(groupName), strings.Join(removedUsersNamesSafe, ", "))

			if _, err := tx.Exec(sql); err != nil {
				// return err
				return fmt.Errorf("Error running SQL query (%s): %w", sql, err)
			}
		}
	}

	if addedUsers.Len() > 0 {
		addedUsersNamesSafe := []string{}
		for _, name := range addedUsers.List() {
			addedUsersNamesSafe = append(addedUsersNamesSafe, pq.QuoteIdentifier(name.(string)))
		}

		sql := fmt.Sprintf("ALTER GROUP %s ADD USER %s", pq.QuoteIdentifier(groupName), strings.Join(addedUsersNamesSafe, ", "))

		if _, err := tx.Exec(sql); err != nil {
			// return err
			return fmt.Errorf("Error running SQL query (%s): %w", sql, err)
		}
	}

	return nil
}

func addMemberUsersNames(tx *sql.Tx, db *DBConnection, d *schema.ResourceData) error {

	groupName := d.Get(groupMembershipNameAttr).(string)
	addedUsers := d.Get(groupMembershipUsersAttr).(*schema.Set)
	if addedUsers.Len() > 0 {
		addedUsersNamesSafe := []string{}
		for _, name := range addedUsers.List() {
			addedUsersNamesSafe = append(addedUsersNamesSafe, pq.QuoteIdentifier(name.(string)))
		}

		sql := fmt.Sprintf("ALTER GROUP %s ADD USER %s", pq.QuoteIdentifier(groupName), strings.Join(addedUsersNamesSafe, ", "))

		if _, err := tx.Exec(sql); err != nil {
			// return err
			return fmt.Errorf("Error running SQL query (%s): %w", sql, err)
		}
	}

	return nil
}

func removeMemberUsersNames(tx *sql.Tx, db *DBConnection, d *schema.ResourceData) error {

	groupName := d.Get(groupMembershipNameAttr).(string)
	var (
		groupUsers []string
	)
	sql := `SELECT ARRAY(SELECT u.usename FROM pg_user_info u, pg_group g WHERE g.grosysid = $1 AND u.usesysid = ANY(g.grolist)) AS members FROM pg_group WHERE grosysid = $1`
	if err := db.QueryRow(sql, d.Id()).Scan(pq.Array(&groupUsers)); err != nil {
		sql = fmt.Sprintf("%s : $1=%s", sql, d.Id())
		return fmt.Errorf("Error running SQL query (%s): %w", sql, err)
	}
	d.Set(groupMembershipUsersAttr, groupUsers)
	removedUsers := d.Get(groupMembershipUsersAttr).(*schema.Set)

	if removedUsers.Len() > 0 {
		removedUsersNamesSafe := []string{}
		for _, name := range removedUsers.List() {
			userExists, err := checkIfMemberUserExists(tx, name.(string))
			if err != nil {
				return err
			}

			if userExists {
				removedUsersNamesSafe = append(removedUsersNamesSafe, pq.QuoteIdentifier(name.(string)))
			}
		}

		if len(removedUsersNamesSafe) > 0 {
			sql := fmt.Sprintf("ALTER GROUP %s DROP USER %s", pq.QuoteIdentifier(groupName), strings.Join(removedUsersNamesSafe, ", "))

			if _, err := tx.Exec(sql); err != nil {
				// return err
				return fmt.Errorf("Error running SQL query (%s): %w", sql, err)
			}
		}
	}
	return nil
}
