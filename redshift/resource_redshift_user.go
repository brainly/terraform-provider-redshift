package redshift

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	userNameAttr           = "name"
	userPasswordAttr       = "password"
	userValidUntilAttr     = "valid_until"
	userCreateDBAttr       = "create_database"
	userConnLimitAttr      = "connection_limit"
	userSyslogAccessAttr   = "syslog_access"
	userSuperuserAttr      = "superuser"
	userSessionTimeoutAttr = "session_timeout"

	// defaults
	defaultUserSyslogAccess          = "RESTRICTED"
	defaultUserSuperuserSyslogAccess = "UNRESTRICTED"
)

// When authenticating using temporary credentials obtained by GetClusterCredentials,
// the resulting username is prefixed with either "IAM:"" or "IAMA:"
// This regexp is designed to match either prefix.
// See https://docs.aws.amazon.com/redshift/latest/APIReference/API_GetClusterCredentials.html
var temporaryCredentialsUsernamePrefixRegexp = regexp.MustCompile("^(?:IAMA?:)")

// Resolve the "real" username by stripping the temporary credentials prefix
func permanentUsername(username string) string {
	return temporaryCredentialsUsernamePrefixRegexp.ReplaceAllString(username, "")
}

func redshiftUser() *schema.Resource {
	return &schema.Resource{
		Description: `
Amazon Redshift user accounts can only be created and dropped by a database superuser. Users are authenticated when they login to Amazon Redshift. They can own databases and database objects (for example, tables) and can grant privileges on those objects to users, groups, and schemas to control who has access to which object. Users with CREATE DATABASE rights can create databases and grant privileges to those databases. Superusers have database ownership privileges for all databases.
`,
		CreateContext: RedshiftResourceFunc(resourceRedshiftUserCreate),
		ReadContext:   RedshiftResourceFunc(resourceRedshiftUserRead),
		UpdateContext: RedshiftResourceFunc(resourceRedshiftUserUpdate),
		DeleteContext: RedshiftResourceFunc(
			RedshiftResourceRetryOnPQErrors(resourceRedshiftUserDelete),
		),
		Exists: RedshiftResourceExistsFunc(resourceRedshiftUserExists),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		CustomizeDiff: func(_ context.Context, d *schema.ResourceDiff, p interface{}) error {
			isSuperuser := d.Get(userSuperuserAttr).(bool)

			isPasswordKnown := d.NewValueKnown(userPasswordAttr)
			password, hasPassword := d.GetOk(userPasswordAttr)
			if isSuperuser && isPasswordKnown && (!hasPassword || password.(string) == "") {
				return fmt.Errorf("Users that are superusers must define a password.")
			}

			isSyslogAccessKnown := d.NewValueKnown(userSyslogAccessAttr)
			syslogAccess, hasSyslogAccess := d.GetOk(userSyslogAccessAttr)
			if isSuperuser && isSyslogAccessKnown && hasSyslogAccess && syslogAccess != defaultUserSuperuserSyslogAccess {
				return fmt.Errorf("Superusers must have syslog access set to %s.", defaultUserSuperuserSyslogAccess)
			}

			return nil
		},

		Schema: map[string]*schema.Schema{
			userNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The name of the user account to create. The user name can't be `PUBLIC`.",
				ValidateFunc: validation.StringNotInSlice([]string{
					"public",
				}, true),
			},
			userPasswordAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				Description: "Sets the user's password. Users can change their own passwords, unless the password is disabled. To disable password, omit this parameter or set it to `null`. Can also be a hashed password rather than the plaintext password. Please refer to the Redshift [CREATE USER documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_USER.html) for information on creating a password hash.",
			},
			userValidUntilAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "infinity",
				Description: "Sets a date and time after which the user's password is no longer valid. By default the password has no time limit.",
			},
			userCreateDBAttr: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Allows the user to create new databases. By default user can't create new databases.",
			},
			userConnLimitAttr: {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      -1,
				Description:  "The maximum number of database connections the user is permitted to have open concurrently. The limit isn't enforced for superusers.",
				ValidateFunc: validation.IntAtLeast(-1),
			},
			userSyslogAccessAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "A clause that specifies the level of access that the user has to the Amazon Redshift system tables and views. If `RESTRICTED` (default) is specified, the user can see only the rows generated by that user in user-visible system tables and views. If `UNRESTRICTED` is specified, the user can see all rows in user-visible system tables and views, including rows generated by another user. `UNRESTRICTED` doesn't give a regular user access to superuser-visible tables. Only superusers can see superuser-visible tables.",
				ValidateFunc: validation.StringInSlice([]string{
					"RESTRICTED",
					"UNRESTRICTED",
				}, false),
				DiffSuppressFunc: func(k, oldValue, newValue string, d *schema.ResourceData) bool {
					if newValue == "" && oldValue == getDefaultSyslogAccess(d) {
						return true
					}
					return false
				},
			},
			userSuperuserAttr: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: `Determine whether the user is a superuser with all database privileges.`,
			},
			userSessionTimeoutAttr: {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      0,
				Description:  "The maximum time in seconds that a session remains inactive or idle. The range is 60 seconds (one minute) to 1,728,000 seconds (20 days). If no session timeout is set for the user, the cluster setting applies.",
				ValidateFunc: validation.All(validation.IntAtLeast(60), validation.IntAtMost(1728000)),
			},
		},
	}
}

func resourceRedshiftUserExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	var name string
	err := db.QueryRow("SELECT usename FROM pg_user_info WHERE usesysid = $1", d.Id()).Scan(&name)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func resourceRedshiftUserCreate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	stringOpts := []struct {
		hclKey string
		sqlKey string
	}{
		{userPasswordAttr, "PASSWORD"},
		{userValidUntilAttr, "VALID UNTIL"},
		{userSyslogAccessAttr, "SYSLOG ACCESS"},
	}

	intOpts := []struct {
		hclKey string
		sqlKey string
	}{
		{userConnLimitAttr, "CONNECTION LIMIT"},
		{userSessionTimeoutAttr, "SESSION TIMEOUT"},
	}

	boolOpts := []struct {
		hclKey        string
		sqlKeyEnable  string
		sqlKeyDisable string
	}{
		{userSuperuserAttr, "CREATEUSER", "NOCREATEUSER"},
		{userCreateDBAttr, "CREATEDB", "NOCREATEDB"},
	}

	createOpts := make([]string, 0, len(stringOpts)+len(intOpts)+len(boolOpts))
	for _, opt := range stringOpts {
		v, ok := d.GetOk(opt.hclKey)
		if !ok {
			if opt.hclKey == userPasswordAttr {
				createOpts = append(createOpts, "PASSWORD DISABLE")
			}

			if opt.hclKey == userSyslogAccessAttr {
				if d.Get(userSuperuserAttr).(bool) {
					createOpts = append(createOpts, "SYSLOG ACCESS UNRESTRICTED")
				} else {
					createOpts = append(createOpts, "SYSLOG ACCESS RESTRICTED")
				}
			}

			continue
		}

		val := v.(string)
		if val != "" {
			switch {
			case opt.hclKey == userPasswordAttr:
				createOpts = append(createOpts, fmt.Sprintf("%s '%s'", opt.sqlKey, pqQuoteLiteral(val)))
			case opt.hclKey == userValidUntilAttr:
				switch {
				case v.(string) == "", strings.ToLower(v.(string)) == "infinity":
					createOpts = append(createOpts, fmt.Sprintf("%s '%s'", opt.sqlKey, "infinity"))
				default:
					createOpts = append(createOpts, fmt.Sprintf("%s '%s'", opt.sqlKey, pqQuoteLiteral(val)))
				}
			case opt.hclKey == userSyslogAccessAttr:
				createOpts = append(createOpts, fmt.Sprintf("%s %s", opt.sqlKey, val))
			default:
				createOpts = append(createOpts, fmt.Sprintf("%s %s", opt.sqlKey, pq.QuoteIdentifier(val)))
			}
		}
	}

	for _, opt := range intOpts {
		val := d.Get(opt.hclKey).(int)
		if opt.hclKey == userSessionTimeoutAttr && val != 0 {
			createOpts = append(createOpts, fmt.Sprintf("%s %d", opt.sqlKey, val))
		} else if opt.hclKey != userSessionTimeoutAttr {
			createOpts = append(createOpts, fmt.Sprintf("%s %d", opt.sqlKey, val))
		}
	}

	for _, opt := range boolOpts {
		val := d.Get(opt.hclKey).(bool)
		valStr := opt.sqlKeyDisable
		if val {
			valStr = opt.sqlKeyEnable
		}
		createOpts = append(createOpts, valStr)
	}

	userName := d.Get(userNameAttr).(string)
	createStr := strings.Join(createOpts, " ")
	sql := fmt.Sprintf("CREATE USER %s WITH %s", pq.QuoteIdentifier(userName), createStr)

	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("error creating user %s: %w", userName, err)
	}

	var usesysid string
	if err := tx.QueryRow("SELECT usesysid FROM pg_user_info WHERE usename = $1", userName).Scan(&usesysid); err != nil {
		return fmt.Errorf("user does not exist in pg_user_info table: %w", err)
	}

	d.SetId(usesysid)

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftUserReadImpl(db, d)
}

func resourceRedshiftUserRead(db *DBConnection, d *schema.ResourceData) error {
	return resourceRedshiftUserReadImpl(db, d)
}

func resourceRedshiftUserReadImpl(db *DBConnection, d *schema.ResourceData) error {
	var userName, userValidUntil, userConnLimit, userSyslogAccess, userSessionTimeout string
	var userSuperuser, userCreateDB bool

	columns := []string{
		"user_name",
		"createdb",
		"superuser",
		"syslog_access",
		`COALESCE(connection_limit::TEXT, 'UNLIMITED')`,
		"session_timeout",
	}

	values := []interface{}{
		&userName,
		&userCreateDB,
		&userSuperuser,
		&userSyslogAccess,
		&userConnLimit,
		&userSessionTimeout,
	}

	useSysID := d.Id()

	userSQL := fmt.Sprintf("SELECT %s FROM svv_user_info WHERE user_id = $1", strings.Join(columns, ","))
	err := db.QueryRow(userSQL, useSysID).Scan(values...)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] Redshift User (%s) not found", useSysID)
		d.SetId("")
		return nil
	case err != nil:
		return fmt.Errorf("Error reading User: %w", err)
	}

	err = db.QueryRow("SELECT COALESCE(valuntil, 'infinity') FROM pg_user_info WHERE usesysid = $1", useSysID).Scan(&userValidUntil)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] Redshift User (%s) not found", useSysID)
		d.SetId("")
		return nil
	case err != nil:
		return fmt.Errorf("Error reading User: %w", err)
	}
	userConnLimitNumber := -1
	if userConnLimit != "UNLIMITED" {
		if userConnLimitNumber, err = strconv.Atoi(userConnLimit); err != nil {
			return err
		}
	}

	userSessionTimeoutNumber, err := strconv.Atoi(userSessionTimeout)
	if err != nil {
		return err
	}

	d.Set(userNameAttr, userName)
	d.Set(userCreateDBAttr, userCreateDB)
	d.Set(userSuperuserAttr, userSuperuser)
	d.Set(userSyslogAccessAttr, userSyslogAccess)
	d.Set(userConnLimitAttr, userConnLimitNumber)
	d.Set(userValidUntilAttr, userValidUntil)
	d.Set(userSessionTimeoutAttr, userSessionTimeoutNumber)

	return nil
}

func resourceRedshiftUserDelete(db *DBConnection, d *schema.ResourceData) error {
	useSysID := d.Id()
	userName := d.Get(userNameAttr).(string)
	newOwnerName := permanentUsername(db.client.config.Username)

	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	// Based on https://github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_find_dropuser_objs.sql
	var reassignOwnerGenerator = `SELECT owner.ddl
			FROM (
			      -- Functions owned by the user
			      SELECT pgu.usesysid,
			      'alter function ' || QUOTE_IDENT(nc.nspname) || '.' ||textin (regprocedureout (pproc.oid::regprocedure)) || ' owner to ' || $2
			      FROM pg_proc pproc,pg_user pgu,pg_namespace nc
			      WHERE pproc.pronamespace = nc.oid
			      AND   pproc.proowner = pgu.usesysid
			  UNION ALL
			      -- Databases owned by the user
			      SELECT pgu.usesysid,
			      'alter database ' || QUOTE_IDENT(pgd.datname) || ' owner to ' || $2
			      FROM pg_database pgd,
				   pg_user pgu
			      WHERE pgd.datdba = pgu.usesysid
			  UNION ALL
			      -- Schemas owned by the user
			      SELECT pgu.usesysid,
			      'alter schema '|| QUOTE_IDENT(pgn.nspname) ||' owner to ' || $2
			      FROM pg_namespace pgn,
				   pg_user pgu
			      WHERE pgn.nspowner = pgu.usesysid
			  UNION ALL
			      -- Tables or Views owned by the user
			      SELECT pgu.usesysid,
			      'alter table ' || QUOTE_IDENT(nc.nspname) || '.' || QUOTE_IDENT(pgc.relname) || ' owner to ' || $2
			      FROM pg_class pgc,
				   pg_user pgu,
				   pg_namespace nc
			      WHERE pgc.relnamespace = nc.oid
			      AND   pgc.relkind IN ('r','v')
			      AND   pgu.usesysid = pgc.relowner
			      AND   nc.nspname NOT ILIKE 'pg\_temp\_%'
			)
			OWNER("userid", "ddl")
			WHERE owner.userid = $1;`

	rows, err := tx.Query(reassignOwnerGenerator, useSysID, pq.QuoteIdentifier(newOwnerName))
	if err != nil {
		return err
	}
	defer rows.Close()

	var reassignStatements []string
	for rows.Next() {
		var statement string
		if err := rows.Scan(&statement); err != nil {
			return err
		}

		reassignStatements = append(reassignStatements, statement)
	}

	for _, statement := range reassignStatements {
		if _, err := tx.Exec(statement); err != nil {
			log.Printf("error: %#v", err)
			return err
		}
	}

	rows, err = tx.Query("SELECT nspname FROM pg_namespace WHERE nspowner != 1 OR nspname = 'public'")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return err
		}

		if _, err := tx.Exec(fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(userName))); err != nil {
			return err
		}

		if _, err := tx.Exec(fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE ALL ON TABLES FROM %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(userName))); err != nil {
			return err
		}

	}

	if _, err := tx.Exec(fmt.Sprintf("DROP USER %s", pq.QuoteIdentifier(userName))); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
		//return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}

func resourceRedshiftUserUpdate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := setUserName(tx, d); err != nil {
		return err
	}

	if err := setUserPassword(tx, d); err != nil {
		return err
	}

	if err := setUserConnLimit(tx, d); err != nil {
		return err
	}

	if err := setUserCreateDB(tx, d); err != nil {
		return err
	}
	if err := setUserSuperuser(tx, d); err != nil {
		return err
	}

	if err := setUserValidUntil(tx, d); err != nil {
		return err
	}

	if err := setUserSyslogAccess(tx, d); err != nil {
		return err
	}

	if err := setUserSessionTimeout(tx, d); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftUserReadImpl(db, d)
}

func setUserName(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(userNameAttr) {
		return nil
	}

	oldRaw, newRaw := d.GetChange(userNameAttr)
	oldValue := oldRaw.(string)
	newValue := newRaw.(string)

	if newValue == "" {
		return fmt.Errorf("Error setting user name to an empty string")
	}

	sql := fmt.Sprintf("ALTER USER %s RENAME TO %s", pq.QuoteIdentifier(oldValue), pq.QuoteIdentifier(newValue))
	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("Error updating User NAME: %w", err)
	}

	return nil
}

func setUserPassword(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(userPasswordAttr) && !d.HasChange(userNameAttr) {
		return nil
	}

	userName := d.Get(userNameAttr).(string)
	password := d.Get(userPasswordAttr).(string)

	passwdTok := "PASSWORD DISABLE"
	if password != "" {
		passwdTok = fmt.Sprintf("PASSWORD '%s'", pqQuoteLiteral(password))
	}

	sql := fmt.Sprintf("ALTER USER %s %s", pq.QuoteIdentifier(userName), passwdTok)
	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("Error updating user password: %w", err)
	}
	return nil
}

func setUserConnLimit(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(userConnLimitAttr) {
		return nil
	}

	connLimit := d.Get(userConnLimitAttr).(int)
	userName := d.Get(userNameAttr).(string)
	sql := fmt.Sprintf("ALTER USER %s CONNECTION LIMIT %d", pq.QuoteIdentifier(userName), connLimit)
	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("Error updating user CONNECTION LIMIT: %w", err)
	}

	return nil
}

func setUserSessionTimeout(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(userSessionTimeoutAttr) {
		return nil
	}

	sessionTimeout := d.Get(userSessionTimeoutAttr).(int)
	userName := d.Get(userNameAttr).(string)
	sql := ""
	if sessionTimeout == 0 {
		sql = fmt.Sprintf("ALTER USER %s RESET SESSION TIMEOUT", pq.QuoteIdentifier(userName))
	} else {
		sql = fmt.Sprintf("ALTER USER %s SESSION TIMEOUT %d", pq.QuoteIdentifier(userName), sessionTimeout)
	}
	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("Error updating user SESSION TIMEOUT: %w", err)
	}

	return nil
}

func setUserCreateDB(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(userCreateDBAttr) {
		return nil
	}

	createDB := d.Get(userCreateDBAttr).(bool)
	tok := "NOCREATEDB"
	if createDB {
		tok = "CREATEDB"
	}
	userName := d.Get(userNameAttr).(string)
	sql := fmt.Sprintf("ALTER USER %s WITH %s", pq.QuoteIdentifier(userName), tok)
	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("Error updating user CREATEDB: %w", err)
	}

	return nil
}

func setUserSuperuser(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(userSuperuserAttr) {
		return nil
	}

	superuser := d.Get(userSuperuserAttr).(bool)
	tok := "NOCREATEUSER"
	if superuser {
		tok = "CREATEUSER"
	}
	userName := d.Get(userNameAttr).(string)
	sql := fmt.Sprintf("ALTER USER %s WITH %s", pq.QuoteIdentifier(userName), tok)
	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("Error updating user SUPERUSER: %w", err)
	}

	return nil
}

func setUserValidUntil(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(userValidUntilAttr) {
		return nil
	}

	validUntil := d.Get(userValidUntilAttr).(string)
	if validUntil == "" {
		return nil
	} else if strings.ToLower(validUntil) == "infinity" {
		validUntil = "infinity"
	}

	userName := d.Get(userNameAttr).(string)
	sql := fmt.Sprintf("ALTER USER %s VALID UNTIL '%s'", pq.QuoteIdentifier(userName), pqQuoteLiteral(validUntil))
	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("Error updating user VALID UNTIL: %w", err)
	}

	return nil
}

func setUserSyslogAccess(tx *sql.Tx, d *schema.ResourceData) error {
	syslogAccessCurrent := d.Get(userSyslogAccessAttr).(string)
	syslogAccessComputed := syslogAccessCurrent
	if syslogAccessComputed == "" {
		syslogAccessComputed = defaultUserSyslogAccess
	}

	if d.Get(userSuperuserAttr).(bool) {
		syslogAccessComputed = defaultUserSuperuserSyslogAccess
	}

	if syslogAccessCurrent == syslogAccessComputed && !d.HasChange(userSyslogAccessAttr) {
		return nil
	}

	userName := d.Get(userNameAttr).(string)
	sql := fmt.Sprintf("ALTER USER %s WITH SYSLOG ACCESS %s", pq.QuoteIdentifier(userName), syslogAccessComputed)
	if _, err := tx.Exec(sql); err != nil {
		return fmt.Errorf("Error updating user SYSLOG ACCESS: %w", err)
	}

	return nil
}

func getDefaultSyslogAccess(d *schema.ResourceData) string {
	if d.Get(userSuperuserAttr).(bool) {
		return defaultUserSuperuserSyslogAccess
	}

	return defaultUserSyslogAccess
}
