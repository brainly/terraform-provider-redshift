# Terraform Provider for AWS Redshift

This provider allows to manage with Terraform [AWS Redshift](https://aws.amazon.com/redshift/) objects like users, groups, schemas, etc..

It's published on the [Terraform registry](https://registry.terraform.io/providers/brainly/redshift/latest/docs).

## Requirements

  - [Terraform](https://www.terraform.io/downloads.html) >= 1.0
  - [Go](https://golang.org/doc/install) 1.21 (to build the provider plugin)

## Building The Provider

```sh
$ git clone git@github.com:brainly/terraform-provider-redshift
```

Enter the provider directory and build the provider

```sh
$ cd terraform-provider-redshift
$ make build
```
## Development

If you're new to provider development, a good place to start is the [Extending
Terraform](https://www.terraform.io/docs/extend/index.html) docs.

### Running Tests

Acceptance tests require a running real AWS Redshift cluster. 

```sh
REDSHIFT_HOST=<cluster ip or DNS>
REDSHIFT_USER=root
REDSHIFT_DATABASE=redshift
REDSHIFT_PASSWORD=<password>
make testacc
```

If your cluster is only accessible from within the VPC, you can connect via a socks proxy:
```sh
ALL_PROXY=socks5[h]://[<socks-user>:<socks-password>@]<socks-host>[:<socks-port>]
NO_PROXY=127.0.0.1,192.168.0.0/24,*.example.com,localhost
```

## Documentation

Documentation is generated with
[tfplugindocs](https://github.com/hashicorp/terraform-plugin-docs). Generated
files are in `docs/` and should not be updated manually. They are derived from:

* Schema `Description` fields in the provider Go code.
* [examples/](./examples)
* [templates/](./templates)

Use `go generate` to update generated docs.

## Releasing

Builds and releases are automated with GitHub Actions and [GoReleaser](https://github.com/goreleaser/goreleaser/). 
The changelog is managed with [github-changelog-generator](https://github.com/github-changelog-generator/github-changelog-generator).

Currently there are a few manual steps to this:

1. Update the changelog:

   ```sh
   RELEASE_VERSION=v... \
   CHANGELOG_GITHUB_TOKEN=... \
   make changelog
   ```

   This will commit the changelog locally.

2. Review generated changelog and push:

   View the committed changelog with `git show`. If all is well `git push origin
   master`.

3. Kick off the release:

   ```sh
   RELEASE_VERSION=v... \
   make release
   ```

   Once the command exits, you can monitor the rest of the process on the
   [Actions UI](https://github.com/brainly/terraform-provider-redshift/actions?query=workflow%3Arelease).

4. Publish release:

   The Action creates the release, but leaves it in "draft" state. Open it up in
   a [browser](https://github.com/brainly/terraform-provider-redshift/releases)
   and if all looks well, click the publish button.
