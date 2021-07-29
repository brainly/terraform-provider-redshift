package redshift

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/customdiff"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func forceNewIfListSizeChanged(key string) schema.CustomizeDiffFunc {
	return customdiff.ForceNewIfChange(key, listSizeChanged)
}

func listSizeChanged(ctx context.Context, old, new, meta interface{}) bool {
	return len(old.([]interface{})) != len(new.([]interface{}))
}
