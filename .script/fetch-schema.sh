#!/bin/bash

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --token)
      TOKEN=$2
      shift 2
      ;;
    --output)
      OUTPUT_FILE=$2
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# If GITHUB_TOKEN is not provided as an argument, check if it's set as an environment variable
if [ -z "${TOKEN}" ]; then
  if [ -z "${GITHUB_TOKEN}" ]; then
    echo "Error: token is not provided as an argument and GITHUB_TOKEN is not set."
    exit 1
  else
    TOKEN=${GITHUB_TOKEN}
  fi
fi

# Check if required arguments are provided
if [ -z "${TOKEN}" ] || [ -z "${OUTPUT_FILE}" ]; then
  echo "Usage: $0 --token GITHUB_TOKEN --output OUTPUT_FILE"
  exit 1
fi

# Fetch the asset URL and download the file
asset_url=$(curl -L -s \
 -H "Accept: application/vnd.github+json" \
 -H "Authorization: Bearer ${TOKEN}" \
 -H "X-GitHub-Api-Version: 2022-11-28" \
 https://api.github.com/repos/raito-io/appserver/releases/latest | jq -r '.assets[] | select(.name=="schema.graphql") | .url ')

curl -L -s \
 -H "Accept: application/octet-stream" \
 -H "Authorization: Bearer ${TOKEN}" \
 -H "X-GitHub-Api-Version: 2022-11-28" \
 ${asset_url} > ${OUTPUT_FILE}