package redshift

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/lib/pq"
)

const (
	roleToAssignAttr = "role_to_assign"
	granteeUserAttr  = "user"
	granteeRoleAttr  = "role"
)

func redshiftRoleGrant() *schema.Resource {
	return &schema.Resource{
		Description: "Manages a Redshift role grant to either a user or another role.",

		Create: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftRoleGrantCreate),
		),
		Read: RedshiftResourceFunc(resourceRedshiftRoleGrantRead),
		Delete: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftRoleGrantDelete),
		),

		Schema: map[string]*schema.Schema{
			roleToAssignAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the role to assign.",
			},
			granteeUserAttr: {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ExactlyOneOf: []string{granteeUserAttr, granteeRoleAttr},
				Description:  "The name of the user to whom the role is being granted.",
			},
			granteeRoleAttr: {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ExactlyOneOf: []string{granteeUserAttr, granteeRoleAttr},
				Description:  "The name of the role to whom the role is being granted.",
			},
		},
	}
}

func resourceRedshiftRoleGrantCreate(db *DBConnection, d *schema.ResourceData) error {
	roleToAssign := d.Get(roleToAssignAttr).(string)

	var grantee, granteeType, query string
	if user, ok := d.GetOk(granteeUserAttr); ok {
		grantee = user.(string)
		granteeType = "USER"
		query = fmt.Sprintf("GRANT ROLE %s TO %s", pq.QuoteIdentifier(roleToAssign), pq.QuoteIdentifier(grantee))
	} else if role, ok := d.GetOk(granteeRoleAttr); ok {
		grantee = role.(string)
		granteeType = "ROLE"
		query = fmt.Sprintf("GRANT ROLE %s TO ROLE %s", pq.QuoteIdentifier(roleToAssign), pq.QuoteIdentifier(grantee))
	} else {
		return fmt.Errorf("one of '%s' or '%s' must be set", granteeUserAttr, granteeRoleAttr)
	}

	log.Printf("[DEBUG] Executing query: %s", query)
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error granting role: %w", err)
	}

	d.SetId(generateRoleGrantID(roleToAssign, granteeType, grantee))
	return resourceRedshiftRoleGrantRead(db, d)
}

func resourceRedshiftRoleGrantRead(db *DBConnection, d *schema.ResourceData) error {
	idParts := strings.Split(d.Id(), "-")
	if len(idParts) != 3 {
		return fmt.Errorf("unexpected ID format (%q), expected roleToAssign-granteeType-grantee", d.Id())
	}

	roleToAssign, granteeType, grantee := idParts[0], idParts[1], idParts[2]

	var query string
	if granteeType == "USER" || granteeType == "" { // Treat empty as USER
		query = `
            SELECT role_name
            FROM svv_user_grants
            WHERE role_name = $1 AND user_name = $2
        `
	} else if granteeType == "ROLE" {
		query = `
            SELECT granted_role_name
            FROM svv_role_grants
            WHERE granted_role_name = $1 AND role_name = $2
        `
	} else {
		return fmt.Errorf("invalid granteeType: %s", granteeType)
	}

	row := db.QueryRow(query, roleToAssign, grantee)
	var fetchedRole string
	if err := row.Scan(&fetchedRole); err == sql.ErrNoRows {
		log.Printf("[DEBUG] Role grant not found: %s -> %s (%s)", roleToAssign, grantee, granteeType)
		d.SetId("") // Clear ID if resource is missing
		return nil
	} else if err != nil {
		return fmt.Errorf("error reading role grant: %w", err)
	}

	log.Printf("[DEBUG] Role grant found: %s -> %s (%s)", roleToAssign, grantee, granteeType)
	return nil
}

func resourceRedshiftRoleGrantDelete(db *DBConnection, d *schema.ResourceData) error {
	idParts := strings.Split(d.Id(), "-")
	if len(idParts) != 3 {
		return fmt.Errorf("unexpected ID format (%q), expected roleToAssign-granteeType-grantee", d.Id())
	}

	roleToAssign, granteeType, grantee := idParts[0], idParts[1], idParts[2]

	var query string
	if granteeType == "USER" || granteeType == "" { // Treat empty as USER
		query = fmt.Sprintf("REVOKE ROLE %s FROM %s", pq.QuoteIdentifier(roleToAssign), pq.QuoteIdentifier(grantee))
	} else if granteeType == "ROLE" {
		query = fmt.Sprintf("REVOKE ROLE %s FROM ROLE %s", pq.QuoteIdentifier(roleToAssign), pq.QuoteIdentifier(grantee))
	} else {
		return fmt.Errorf("invalid granteeType: %s", granteeType)
	}

	log.Printf("[DEBUG] Executing query: %s", query)
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error revoking role: %w", err)
	}

	d.SetId("")
	return nil
}
func generateRoleGrantID(roleToAssign, granteeType, grantee string) string {
	return fmt.Sprintf("%s-%s-%s", roleToAssign, granteeType, grantee)
}
