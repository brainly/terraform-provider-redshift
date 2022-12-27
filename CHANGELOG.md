# Changelog

## [v1.0.4](https://github.com/brainly/terraform-provider-redshift/tree/v1.0.4) (2022-12-27)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v1.0.3...v1.0.4)

**Merged pull requests:**

- Add SESSION TIMEOUT support and missing defer rows.Close\(\) [\#101](https://github.com/brainly/terraform-provider-redshift/pull/101) ([robertomczak](https://github.com/robertomczak))

## [v1.0.3](https://github.com/brainly/terraform-provider-redshift/tree/v1.0.3) (2022-11-15)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v1.0.2...v1.0.3)

**Fixed bugs:**

- Non-idempotent reapplying grants for entities with ascii chars 33-126 in their names [\#94](https://github.com/brainly/terraform-provider-redshift/issues/94)

**Closed issues:**

- Error when remove user from group [\#83](https://github.com/brainly/terraform-provider-redshift/issues/83)

**Merged pull requests:**

- Fix - preventing from non-idempotent reapplying grants for entities with ascii chars 33-126 in their names [\#93](https://github.com/brainly/terraform-provider-redshift/pull/93) ([rg00d](https://github.com/rg00d))
- Improve docs for redshift\_datashare\_privilege resource [\#77](https://github.com/brainly/terraform-provider-redshift/pull/77) ([szemek](https://github.com/szemek))

## [v1.0.2](https://github.com/brainly/terraform-provider-redshift/tree/v1.0.2) (2022-09-02)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v1.0.1...v1.0.2)

**Fixed bugs:**

- Fix removing existing users from groups [\#81](https://github.com/brainly/terraform-provider-redshift/pull/81) ([winglot](https://github.com/winglot))

## [v1.0.1](https://github.com/brainly/terraform-provider-redshift/tree/v1.0.1) (2022-08-04)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v1.0.0...v1.0.1)

**Fixed bugs:**

- Fix readSchemaGrants and readTableGrants for groups [\#76](https://github.com/brainly/terraform-provider-redshift/pull/76) ([olivermeyer](https://github.com/olivermeyer))

## [v1.0.0](https://github.com/brainly/terraform-provider-redshift/tree/v1.0.0) (2022-04-22)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v0.6.0...v1.0.0)

**Implemented enhancements:**

- Ability to grant EXECUTE function [\#34](https://github.com/brainly/terraform-provider-redshift/issues/34)
- Add rule and trigger permissions to grant and default\_grant [\#63](https://github.com/brainly/terraform-provider-redshift/pull/63) ([mtesch-um](https://github.com/mtesch-um))
- Add `region` support when calling `GetClusterCredentials` [\#60](https://github.com/brainly/terraform-provider-redshift/pull/60) ([bendrucker](https://github.com/bendrucker))
- Support language, function and procedure object types in redshift\_grant resource [\#57](https://github.com/brainly/terraform-provider-redshift/pull/57) ([pawelaugustyn](https://github.com/pawelaugustyn))

**Fixed bugs:**

- Quoted user names are now always case-sensitive regardless of enable\_case\_sensitive\_identifier setting [\#67](https://github.com/brainly/terraform-provider-redshift/issues/67)
- `user`: handle unknown superuser passwords [\#59](https://github.com/brainly/terraform-provider-redshift/pull/59) ([bendrucker](https://github.com/bendrucker))

**Closed issues:**

- "grant ALL on table.." grants more permissions than available in redshift\_grant [\#62](https://github.com/brainly/terraform-provider-redshift/issues/62)
- redshift\_grant resource does not support language object [\#45](https://github.com/brainly/terraform-provider-redshift/issues/45)

**Merged pull requests:**

- Preserve user names case in group membership list and database owner attribute [\#69](https://github.com/brainly/terraform-provider-redshift/pull/69) ([winglot](https://github.com/winglot))
- Make sure quoted user name case is preserved in state [\#68](https://github.com/brainly/terraform-provider-redshift/pull/68) ([winglot](https://github.com/winglot))
- Before alter group make sure user still exists [\#66](https://github.com/brainly/terraform-provider-redshift/pull/66) ([lukasbudisky](https://github.com/lukasbudisky))

## [v0.6.0](https://github.com/brainly/terraform-provider-redshift/tree/v0.6.0) (2022-02-16)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v0.5.3...v0.6.0)

**Implemented enhancements:**

- Not able to GetClusterCredentials using cross account [\#46](https://github.com/brainly/terraform-provider-redshift/issues/46)
- redshift\_grant resource does not support granting privileges to users [\#23](https://github.com/brainly/terraform-provider-redshift/issues/23)

**Fixed bugs:**

- non-unique ids in redshift\_grant possible [\#43](https://github.com/brainly/terraform-provider-redshift/issues/43)
- Issue with revoking privileges while droping user [\#35](https://github.com/brainly/terraform-provider-redshift/issues/35)

**Removed:**

- Remove previously deprecated redshift\_privilege resource [\#54](https://github.com/brainly/terraform-provider-redshift/pull/54) ([winglot](https://github.com/winglot))

**Merged pull requests:**

- Update documentation with recent changes [\#55](https://github.com/brainly/terraform-provider-redshift/pull/55) ([winglot](https://github.com/winglot))
- Support for users in redshift\_default\_privileges resources [\#53](https://github.com/brainly/terraform-provider-redshift/pull/53) ([winglot](https://github.com/winglot))
- Support for users in redshift\_grant resource [\#52](https://github.com/brainly/terraform-provider-redshift/pull/52) ([winglot](https://github.com/winglot))
- Fixed non-unique ids in redshift\_grant possible [\#51](https://github.com/brainly/terraform-provider-redshift/pull/51) ([rg00d](https://github.com/rg00d))
- Support cross-account temporary credentials fetching [\#50](https://github.com/brainly/terraform-provider-redshift/pull/50) ([pawelaugustyn](https://github.com/pawelaugustyn))

## [v0.5.3](https://github.com/brainly/terraform-provider-redshift/tree/v0.5.3) (2022-01-26)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v0.5.2...v0.5.3)

**Merged pull requests:**

- Fix typo in code - wrong variable referenced [\#48](https://github.com/brainly/terraform-provider-redshift/pull/48) ([matokovacik](https://github.com/matokovacik))

## [v0.5.2](https://github.com/brainly/terraform-provider-redshift/tree/v0.5.2) (2022-01-26)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v0.5.1...v0.5.2)

**Merged pull requests:**

- Remove CASCADE from ALTER DEFAULT when dropping users and groups [\#47](https://github.com/brainly/terraform-provider-redshift/pull/47) ([winglot](https://github.com/winglot))
- Move the superuser/password validation to CustomizeDiff func [\#44](https://github.com/brainly/terraform-provider-redshift/pull/44) ([winglot](https://github.com/winglot))
- added import docs and warning to privilege description [\#41](https://github.com/brainly/terraform-provider-redshift/pull/41) ([mtesch-um](https://github.com/mtesch-um))

## [v0.5.1](https://github.com/brainly/terraform-provider-redshift/tree/v0.5.1) (2021-12-30)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v0.5.0...v0.5.1)

**Implemented enhancements:**

- redshift does not allow create superuser without password [\#37](https://github.com/brainly/terraform-provider-redshift/issues/37)

**Merged pull requests:**

- Deprecate redshift\_privilege resource [\#40](https://github.com/brainly/terraform-provider-redshift/pull/40) ([winglot](https://github.com/winglot))
- Require password for superuser at plan phase [\#39](https://github.com/brainly/terraform-provider-redshift/pull/39) ([winglot](https://github.com/winglot))

## [v0.5.0](https://github.com/brainly/terraform-provider-redshift/tree/v0.5.0) (2021-12-10)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v0.4.3...v0.5.0)

**Implemented enhancements:**

- feat: Add drop privilege for table objects. [\#33](https://github.com/brainly/terraform-provider-redshift/pull/33) ([taylorrm1](https://github.com/taylorrm1))

## [v0.4.3](https://github.com/brainly/terraform-provider-redshift/tree/v0.4.3) (2021-11-22)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v0.4.2...v0.4.3)

**Fixed bugs:**

- redshift\_grant unable to detect drift on view grants [\#30](https://github.com/brainly/terraform-provider-redshift/issues/30)
- BUG `create\_external\_database\_if\_not\_exists` looks like not working. [\#27](https://github.com/brainly/terraform-provider-redshift/issues/27)
- `redshift\_grants` resource occasionally shows incorrect diffs or permanent diffs [\#24](https://github.com/brainly/terraform-provider-redshift/issues/24)

**Merged pull requests:**

- Rewrite DiffSuppressFunc for create\_external\_database\_if\_not\_exists. [\#32](https://github.com/brainly/terraform-provider-redshift/pull/32) ([winglot](https://github.com/winglot))
- Extend grant detection also to views and materialized views. [\#31](https://github.com/brainly/terraform-provider-redshift/pull/31) ([matokovacik](https://github.com/matokovacik))

## [v0.4.2](https://github.com/brainly/terraform-provider-redshift/tree/v0.4.2) (2021-10-12)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v0.4.1...v0.4.2)

**Fixed bugs:**

- Fixed dynamic password resolution for provider [\#29](https://github.com/brainly/terraform-provider-redshift/pull/29) ([marekpp](https://github.com/marekpp))

## [v0.4.1](https://github.com/brainly/terraform-provider-redshift/tree/v0.4.1) (2021-09-23)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v0.4.0...v0.4.1)

**Fixed bugs:**

- Fix reading group grant privileges [\#26](https://github.com/brainly/terraform-provider-redshift/pull/26) ([winglot](https://github.com/winglot))

## [v0.4.0](https://github.com/brainly/terraform-provider-redshift/tree/v0.4.0) (2021-09-06)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v0.3.0...v0.4.0)

**Implemented enhancements:**

- Implement redshift\_default\_privileges resource [\#21](https://github.com/brainly/terraform-provider-redshift/pull/21) ([winglot](https://github.com/winglot))
- Manage datashare consumer permissions to use datashares [\#20](https://github.com/brainly/terraform-provider-redshift/pull/20) ([sworisbreathing](https://github.com/sworisbreathing))
- Support connecting through a socks proxy [\#19](https://github.com/brainly/terraform-provider-redshift/pull/19) ([sworisbreathing](https://github.com/sworisbreathing))
- Add simple `redshift\_datashare` resource [\#18](https://github.com/brainly/terraform-provider-redshift/pull/18) ([sworisbreathing](https://github.com/sworisbreathing))
- Add `redshift\_namespace` data source [\#16](https://github.com/brainly/terraform-provider-redshift/pull/16) ([sworisbreathing](https://github.com/sworisbreathing))

**Fixed bugs:**

- Default privileges approach is incorrect [\#15](https://github.com/brainly/terraform-provider-redshift/issues/15)

**Merged pull requests:**

- Add `redshift\_grant` resource [\#22](https://github.com/brainly/terraform-provider-redshift/pull/22) ([winglot](https://github.com/winglot))

## [v0.3.0](https://github.com/brainly/terraform-provider-redshift/tree/v0.3.0) (2021-08-06)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v0.2.0...v0.3.0)

**Implemented enhancements:**

- Add external schema support [\#14](https://github.com/brainly/terraform-provider-redshift/pull/14) ([sworisbreathing](https://github.com/sworisbreathing))
- Add `redshift\_database` resource and data source. [\#12](https://github.com/brainly/terraform-provider-redshift/pull/12) ([sworisbreathing](https://github.com/sworisbreathing))

## [v0.2.0](https://github.com/brainly/terraform-provider-redshift/tree/v0.2.0) (2021-07-28)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v0.1.1...v0.2.0)

**Implemented enhancements:**

- Add `redshift\_schema` data source [\#10](https://github.com/brainly/terraform-provider-redshift/pull/10) ([sworisbreathing](https://github.com/sworisbreathing))
- Use md5 hashing for redshift\_user passwords [\#7](https://github.com/brainly/terraform-provider-redshift/pull/7) ([sworisbreathing](https://github.com/sworisbreathing))
- Add `redshift\_user` data source [\#6](https://github.com/brainly/terraform-provider-redshift/pull/6) ([sworisbreathing](https://github.com/sworisbreathing))
- Add `redshift\_group` data source [\#5](https://github.com/brainly/terraform-provider-redshift/pull/5) ([sworisbreathing](https://github.com/sworisbreathing))
- Support provider authentication using GetClusterCredentials [\#3](https://github.com/brainly/terraform-provider-redshift/pull/3) ([sworisbreathing](https://github.com/sworisbreathing))

**Fixed bugs:**

- Bug fixes related to temporary credentials [\#11](https://github.com/brainly/terraform-provider-redshift/pull/11) ([sworisbreathing](https://github.com/sworisbreathing))
- Fix error changing ownership when deleting a user, if the current user has temporary credentials [\#4](https://github.com/brainly/terraform-provider-redshift/pull/4) ([sworisbreathing](https://github.com/sworisbreathing))

**Merged pull requests:**

- Fix typo `provider.go` [\#13](https://github.com/brainly/terraform-provider-redshift/pull/13) ([rafrafek](https://github.com/rafrafek))
- Add GitHub action to run basic tests [\#8](https://github.com/brainly/terraform-provider-redshift/pull/8) ([winglot](https://github.com/winglot))

## [v0.1.1](https://github.com/brainly/terraform-provider-redshift/tree/v0.1.1) (2021-06-25)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/v0.1.0...v0.1.1)

**Fixed bugs:**

- Convert state values to lower case to produce consistent state during plan [\#2](https://github.com/brainly/terraform-provider-redshift/pull/2) ([winglot](https://github.com/winglot))
- Retry on errors for Create and Update operations in redshift\_privilege [\#1](https://github.com/brainly/terraform-provider-redshift/pull/1) ([winglot](https://github.com/winglot))

## [v0.1.0](https://github.com/brainly/terraform-provider-redshift/tree/v0.1.0) (2021-06-23)

[Full Changelog](https://github.com/brainly/terraform-provider-redshift/compare/26f7484819a65eff27ceebc5350371a556d305d3...v0.1.0)



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
