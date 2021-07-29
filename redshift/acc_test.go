// This file shouldn't contain actual test cases,
// but rather common utility methods for acceptance tests.
package redshift

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// Get the value of an environment variable, or skip the
// current test if the variable is not set.
func getEnvOrSkip(key string, t *testing.T) string {
	v := os.Getenv(key)
	if v == "" {
		t.Skipf(fmt.Sprintf("Environment variable %s was not set. Skipping...", key))
	}
	return v
}

// Renders a string slice as a terraform array
func tfArray(s []string) string {
	semiformat := fmt.Sprintf("%q\n", s)
	tokens := strings.Split(semiformat, " ")
	return fmt.Sprintf(strings.Join(tokens, ","))
}
