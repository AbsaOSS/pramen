# syncwatcher-py-examples

Example package containing transformers for
<https://github.com/absa-group/syncwatcher-py/>

## Development

Prerequisites:
- <https://python-poetry.org/docs/#installation>
- python 3.6

Setup steps:

```bash
git clone https://github.com/absa-group/syncwatcher-py
make install  # create virtualenv and install dependencies
make test
make pre-commit
```


### Load environment configuration

Before doing any development step, you have to set your development
environment variables

```bash
make install
```

## Deployment

### From the local development environment

```bash
# bump the version
vim pyproject.toml

# deploy to the asgard (included steps of building and publishing
#   artefacts)
cat .env.asgard
make publish
DEPLOY_ENV=asgard make release
```

### CI&CD with the GitHub actions

Each deployment environment (i.e. asgard, binks etc.) should have corresponding
file with secrets. The filename should follow the following pattern:
.env.{{ environment name }}. This file should be encrypted
(see [Working with secrets](#working-with-secrets))

There is .env.example with the documentation in the project root.

### Working with secrets

For secrets management https://git-secret.io/ is used.
In order to get access to the repo secrets, it is required
to submit a ticket in the repo and also provide a public
gpg key.

The repo keyring will be updated and access will be provided.

Basic workflow is
```bash
# Working with secrets
# git-secret.io should be installed

git secret reveal  # to reveal secrets
# optionally modify the content of secrets (add, delete, edit)
git secret hide -d # to encrypt secrets and delete revealed one
# commit your changes

# Adding new user to access the keys
gpg --import artem_public_key.gpg # in case the key was provided as a file
gpg --receive-keys artem.zhukov@absa.africa # in case the key is available on a key server
git secret tell artem.zhukov@absa.africa # to share a key with Artem
git secret reveal && git secret hide -d # to reencrypt the secrets
git secret whoknows # to see who has access to the secrets
```
