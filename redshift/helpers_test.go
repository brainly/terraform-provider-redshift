package redshift

import (
	"testing"
)

func TestValidatePrivileges(t *testing.T) {
	tests := map[string]struct {
		privileges []string
		objectType string
		expected   bool
	}{
		"valid list for schema": {
			privileges: []string{"create", "usage", "alter"},
			objectType: "schema",
			expected:   true,
		},
		"invalid list for schema": {
			privileges: []string{"foo"},
			objectType: "schema",
			expected:   false,
		},
		"extended invalid list for schema": {
			privileges: []string{"create", "usage", "insert"},
			objectType: "schema",
			expected:   false,
		},
		"empty list for schema": {
			privileges: []string{},
			objectType: "schema",
			expected:   true,
		},
		"valid list for table": {
			privileges: []string{"insert", "update", "delete", "select", "drop", "references", "rule", "trigger", "alter", "truncate"},
			objectType: "table",
			expected:   true,
		},
		"invalid list for table": {
			privileges: []string{"foobar"},
			objectType: "schema",
			expected:   false,
		},
		"extended invalid list for table": {
			privileges: []string{"create", "usage", "insert"},
			objectType: "table",
			expected:   false,
		},
		"empty list for table": {
			privileges: []string{},
			objectType: "table",
			expected:   true,
		},
		"valid list for function": {
			privileges: []string{"execute"},
			objectType: "function",
			expected:   true,
		},
		"invalid list for function": {
			privileges: []string{"foo"},
			objectType: "function",
			expected:   false,
		},
		"extended invalid list for function": {
			privileges: []string{"execute", "foo"},
			objectType: "function",
			expected:   false,
		},
		"valid list for procedure": {
			privileges: []string{"execute"},
			objectType: "procedure",
			expected:   true,
		},
		"invalid list for procedure": {
			privileges: []string{"foo"},
			objectType: "procedure",
			expected:   false,
		},
		"extended invalid list for procedure": {
			privileges: []string{"execute", "foo"},
			objectType: "procedure",
			expected:   false,
		},
		"valid list for language": {
			privileges: []string{"usage"},
			objectType: "language",
			expected:   true,
		},
		"invalid list for language": {
			privileges: []string{"foo"},
			objectType: "language",
			expected:   false,
		},
		"extended invalid list for language": {
			privileges: []string{"usage", "foo"},
			objectType: "language",
			expected:   false,
		},
		"empty list for language": {
			privileges: []string{},
			objectType: "language",
			expected:   false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			result := validatePrivileges(tt.privileges, tt.objectType)

			if result != tt.expected {
				t.Errorf("Expected result to be `%t` but got `%t`", tt.expected, result)
			}
		})
	}
}
