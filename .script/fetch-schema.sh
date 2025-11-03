#!/bin/bash

# ==========================================================
# This script downloads a GraphQL schema from a running
# service using an introspection query and Basic Authentication.
# How to use:
# ./fetch-schema.sh --login "user" --password "P4sWorD" --output "./result.txt" --url "https://access-governance.collibra.tech"
# NOTE:
# --url is optional. If not provided the "https://access-governance.collibra.tech" will be used
# ==========================================================

# --- Argument Parsing ---
# Initialize variables to hold login, password, output file, and optional URL
LOGIN=""
PASSWORD=""
OUTPUT_FILE=""
URL_OVERRIDE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --login)
      LOGIN=$2
      shift 2
      ;;
    --password)
      PASSWORD=$2
      shift 2
      ;;
    --output)
      OUTPUT_FILE=$2
      shift 2
      ;;
    --url)
      URL_OVERRIDE=$2
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# --- URL and Validation ---
DEFAULT_URL="https://access-governance.collibra.tech/dataAccess/query"
SERVICE_URL=${URL_OVERRIDE:-$DEFAULT_URL}

# Ensure the URL has the correct GraphQL endpoint path
GRAPHQL_PATH="/dataAccess/query"
if [[ ! "${SERVICE_URL}" =~ "${GRAPHQL_PATH}"$ ]]; then
  # Append the path if it's missing
  SERVICE_URL="${SERVICE_URL}${GRAPHQL_PATH}"
fi

# Check if all required arguments are provided
if [ -z "${LOGIN}" ] || [ -z "${PASSWORD}" ] || [ -z "${OUTPUT_FILE}" ]; then
  echo "Error: Missing arguments."
  echo "Usage: $0 --login COLLIBRA_USERNAME --password COLLIBRA_PASSWORD --output OUTPUT_FILE [--url CUSTOM_URL]"
  exit 1
fi

# --- Downloading the File using GraphQL Introspection ---
AUTH_HEADER="Authorization: Basic $(echo -n "${LOGIN}:${PASSWORD}" | base64)"

npx --yes @apollo/rover graph introspect "${SERVICE_URL}" \
  --header "${AUTH_HEADER}" \
  --output "${OUTPUT_FILE}"

# Add a success message
echo "Successfully downloaded GraphQL schema to ${OUTPUT_FILE}"
