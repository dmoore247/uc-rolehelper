# UC Role Helper
A solution accelerator to help customers implement 'role' switching.

## Project Description
Taking Azure Databricks example from Liran Baret and converting it to run on AWS with the use of service principals and client_id, client_secrets (OAuth Secrets, not OAuth tokens).

## Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.

Please reach out to your local Data Governance Specialilst.

## Using the Project

### Pre-requisites
- UC Enabled workspace
- UC Enabled cluster ( > DBR 13.3 LTS )
- Account Level Service Principal (Admin entitlement) w/ OAUTH secret
- 3 to N 'role' level groups. These account level groups will govern access to data and compute resources. A service principal will be dynamically assigned to these, one at a time.
- One 'role-switcher' group, these are the principals authorized to use the role-switcher solution accelerator.
- Networking: Customer dataplane to databricks control plane network path for https REST API calls
- Group Management:
  - Remove users from other mechanisms to the groups that roleswitcher manages through thoughtful design of the groups
- Access Management:
  - Remove alternate access to protected resources via groups outside of roleswitcher (remove side doors and backdoors)

## Deploying / Installing the Project
Instructions for how to deploy the project, or install it
- Fork the github repo, add the REPO to your workspace
- Download [config.json](./config.json),
  - update the configuration information,
  - use [`load-config.sh`](./load-config.sh) to load that into a secret scope=`uc-rolehelper`, key=`config` (Do not commit your version to github as it may contain secrets).
- Open `notebooks/Role Switch Demo`
- Attach to UC enabled cluster
- Run

## Building the Project
Instructions for how to build the project

After an edit, run `make test`
Before commit, run `make check`
Consult the `Makefile` for other build options.

## Releasing the Project
Instructions for how to release a version of the project

```bash
pyenv virtualenv uc-rolehelper
pyenv activate uc-rolehelper
make install-dev
make style
make test
```
