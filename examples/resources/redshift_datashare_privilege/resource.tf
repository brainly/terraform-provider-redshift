resource "redshift_datashare" "share" {
  name = "my_datashare"
}

# Example: datashare permission in same account
resource "redshift_datashare_privilege" "within_account" {
  share_name = redshift_datashare.share.name          # Required
  namespace  = "d34dbe3f-d34d-b33f-d3ad-b33fd34db33f" # Required
}

# Example: cross-account datashare permission.
# Note: you will also need to authorize the cross-account datashare
# in the AWS console after creating this resource
resource "redshift_datashare_privilege" "cross_account" {
  share_name = redshift_datashare.share.name # Required
  account    = "123456789012"                # Required
}
