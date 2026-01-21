# DE ZOOCAMP 2026

[Zoocamp Github](https://github.com/DataTalksClub/data-engineering-zoomcamp?tab=readme-ov-file)

## TERRAFORM

### 1 - Set creds

Get the credentials in the GCP creating a service account

### 2 - Make the `main.tf` file basic

Add the `.tf` file and create the base project, set the credential, in my case I hard coded, but you can set the ENV variable to the path or use gcloud login

### 3 - Terraform init

Use the command `terraform init` to get the information based in the Cloud Provider or start the empty folder for terraform

### 4 - Create the resources

Create the resources in the `.tf` file and run `terraform plan` to check the changes that are goig to be applied

## 5 - Apply the changes

Check if the plan is correct and run the `terraform apply` command.

## 6 - Destroy

Run the `terraform destroy` to delete all
