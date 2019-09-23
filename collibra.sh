#!/bin/bash

export COLLIBRA_COMMUNITY_DESCRIPTION="VirtualBox Linux Community"
export COLLIBRA_COMMUNITY_NAME="Linux Community"
export COLLIBRA_URL="https://experian-dev-54.collibra.com"
export COLLIBRA_USERNAME="Admin"
export COLLIBRA_PASSWORD="Password123"
export HTTP_ODBC_URL="http://169.254.0.20:8001"
export HTTP_ODBC_RULE_QUERY="SELECT * FROM \"RULES\""
export HTTP_ODBC_PROFILE_QUERY="SELECT * FROM \"PROFILES\""
export COLLIBRA_DEBUG_LEVEL="debug"

node lib/collibra/collibra-connector.js
