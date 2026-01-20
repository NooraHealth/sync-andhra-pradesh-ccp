[![sync_andhra_pradesh_ccp](https://github.com/NooraHealth/ap-ccp-cron/actions/workflows/sync_andhra_pradesh_ccp.yaml/badge.svg)](https://github.com/NooraHealth/ap-ccp-cron/actions/workflows/sync_andhra_pradesh_ccp.yaml)

[![sync_andhra_pradesh_mlhp](https://github.com/NooraHealth/ap-ccp-cron/actions/workflows/sync_andhra_pradesh_mlhp.yaml/badge.svg)](https://github.com/NooraHealth/ap-ccp-cron/actions/workflows/sync_andhra_pradesh_mlhp.yaml)

# Overview

This repository contains GitHub Actions workflows that fetch data from the Andhra Pradesh CCP API and the Andhra Pradesh MLHP API and sync the data to the BigQuery data warehouse.

The code fetches "back-dated" records, i.e., where the submission date is later than the session date. The data are deduplicated in dbt.

## Setup

1. Set up uv for managing dependencies.
   1. Install uv according to instructions [here](https://docs.astral.sh/uv/getting-started/installation/).
   2. Install python using `uv python install`.
   3. Sync the project's dependencies using `uv sync`.
   4. Add and remove dependencies using `uv add package_name` and `uv remove package_name`.
   5. Run python scripts using `uv run path_to_script.py`.

2. Configure the repository secrets locally.
   1. Create a file secrets/andhra_pradesh_ccp.yml as follows, replacing `xxx` as appropriate:

      ```yaml
      url: xxx
      username: xxx
      password: xxx
      ```
      The `url` is currently a proxy URL, because the actual API is inaccessible within GitHub Actions due to an unresolvable misconfiguration on the API server.
   2. Create a file secrets/andhra_pradesh_mlhp.yml as follows, replacing `xxx` as appropriate:

      ```yaml
      url: xxx
      key: xxx
      ```
   3. Create a file secrets/slack_token.txt that contains the Slack bot token.
   4. Create a file secrets/service_account_key.json that contains the JSON key for the service account that will connect to BigQuery (airbyte-user).
   5. Create a file secrets/gh_pat.txt that contains a GitHub fine-grained personal access token that has
      - access on the NooraHealth organization
      - repository access to this repo
      - read and write permissions for Actions

3. Configure the repository secrets on GitHub.
   1. Copy and paste the contents of secrets/andhra_pradesh_ccp.yml into a secret named "ANDHRA_PRADESH_CCP".
   2. Copy and paste the contents of secrets/andhra_pradesh_mlhp.yml into a secret named "ANDHRA_PRADESH_MLHP".
   3. Copy and paste the contents of secrets/slack_token.txt into a secret named "SLACK_TOKEN".
   4. Copy and paste the contents of secrets/bigquery_service_account_key.json into a secret named "SERVICE_ACCOUNT_KEY".
   5. Copy and paste the contents of secrets/gh_pat.txt into a secret named "GH_PAT".
