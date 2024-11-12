package main

import (
	"flag"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"

	"redshifttf/redshift"
)

//go:generate go run github.com/hashicorp/terraform-plugin-docs/cmd/tfplugindocs

func main() {
    var debug bool

    flag.BoolVar(&debug, "debug", false, "set to true to run the provider with support for debuggers like delve")
    flag.Parse()

	plugin.Serve(&plugin.ServeOpts{
		Debug : debug,
		ProviderAddr: "jt.dev/tf/redshifttf",
		ProviderFunc: func() *schema.Provider {
			return redshift.Provider()
		},
	})
}
