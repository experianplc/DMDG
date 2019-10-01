@echo off
setlocal 
  set COLLIBRA_COMMUNITY_NAME=""
  set COLLIBRA_COMMUNITY_DESCRIPTION=""
  set COLLIBRA_URL=""
  set COLLIBRA_USERNAME=""
  set COLLIBRA_PASSWORD=""
  set HTTP_ODBC_URL=""
  set HTTP_ODBC_RULE_QUERY=""
  set HTTP_ODBC_PROFILE_QUERY=""
  set COLLIBRA_DEBUG_LEVEL="debug"
  set COLLIBRA_MULTI_COMMUNITY=false
  collibra-connector-win.exe
endlocal
