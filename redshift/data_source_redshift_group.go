package redshift

import (
	"regexp"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

func dataSourceRedshiftGroup() *schema.Resource {
	return &schema.Resource{
		Description: `
Groups are collections of users who are all granted whatever privileges are associated with the group. You can use groups to assign privileges by role. For example, you can create different groups for sales, administration, and support and give the users in each group the appropriate access to the data they require for their work. You can grant or revoke privileges at the group level, and those changes will apply to all members of the group, except for superusers.
		`,
		ReadContext: RedshiftResourceFunc(dataSourceRedshiftGroupRead),
		Schema: map[string]*schema.Schema{
			groupNameAttr: {
				Type:         schema.TypeString,
				Required:     true,
				Description:  "Name of the user group. Group names beginning with two underscores are reserved for Amazon Redshift internal use.",
				ValidateFunc: validation.StringDoesNotMatch(regexp.MustCompile("^__.*"), "Group names beginning with two underscores are reserved for Amazon Redshift internal use"),
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			groupUsersAttr: {
				Type:     schema.TypeSet,
				Computed: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Description: "List of the user names who belong to the group",
			},
		},
	}
}

func dataSourceRedshiftGroupRead(db *DBConnection, d *schema.ResourceData) error {
	var (
		groupId    string
		groupUsers []string
	)

	sql := `SELECT ARRAY(SELECT u.usename FROM pg_user_info u, pg_group g WHERE g.groname = $1 AND u.usesysid = ANY(g.grolist)) AS members, grosysid FROM pg_group WHERE groname = $1`
	if err := db.QueryRow(sql, d.Get(groupNameAttr).(string)).Scan(pq.Array(&groupUsers), &groupId); err != nil {
		return err
	}

	d.SetId(groupId)
	d.Set(groupUsersAttr, groupUsers)
	return nil
}
