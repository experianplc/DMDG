import Connector from "./../connector";
import axios from "axios";
import Promise from "bluebird";
import FormData from "form-data";

/**
 * Prints and logs a message
 *
 * _Example_:
 * ```typescript
 *  log("Debug", 5);
 *  log("Info", 4);
 *  log("Warning!", 3);
 *  log("ERROR!!", 2);
 *  log("FATAL - Closing...", 1);
 * ```
 *
 * @param message - The message to be logged
 * @param level - The level of the message
 * @returns void
 */
function log(message: string, level: number | string = 1): void {
  const levels = [
    "none",
    "fatal",
    "error",
    "warning",
    "info",
    "debug"
  ];

  if (typeof level === "string") {
    level = levels.indexOf(level);
  }

  let debugLevel = 3;
  const collibraEnvLevel = process.env.COLLIBRA_DEBUG_LEVEL;
  if (typeof collibraEnvLevel === "string") {
    const levelsIndex = levels.indexOf(collibraEnvLevel);
    if (levelsIndex === -1) {
      throw `COLLIBRA_DEBUG_LEVEL should be one of:
      none, fatal, error, warning, info or debug`;
    } else {
      debugLevel = levelsIndex;
    }
  } else if (typeof collibraEnvLevel === "number") {
    debugLevel = Number(collibraEnvLevel);
  }

  if (debugLevel >= level) {
    console.log(`${levels[level].toUpperCase()}: ${message}`);
  }
}

function setEnvVar(context: any, variable: string, required: boolean = false, defaultValue: any = undefined) {
  if (!process.env[variable]) {
    if (!process.env[variable] && required) {
      log(`The value: ${variable} was improperly or not set.`, "warning");
      throw `${variable} must be set to continue. Exiting...`;
    }

    if (defaultValue) {
      process.env[variable] = defaultValue;
      log(`Defaulting to: ${defaultValue}`, "info");
    }
  }

  context[variable] = process.env[variable];
}


/**
 * A mapping between Job IDs and arguments, for debugging.
 */
let ids: any = {};

export class CollibraConnector extends Connector {
  // Used to make subsequent API requests to the Collibra Data Governance Center API.
  cookie: string;

  // Custom relation types
  customRelations: any; 

  /**
   * See [CollibraEnvironmentVariables]
   */
  env: CollibraEnvironmentVariables;

  constructor(args: ConstructorArgs) {
    super();
    this.cookie = "";
    this.customRelations = {};
    this.env = <CollibraEnvironmentVariables> {};

    setEnvVar(this.env, "COLLIBRA_URL", true);
    setEnvVar(this.env, "COLLIBRA_LAST_RUN", false, new Date("1900-01-01"));
    setEnvVar(this.env, "HTTP_ODBC_URL", true);
    setEnvVar(this.env, "COLLIBRA_COMMUNITY_NAME", false, "Experian Community");
    setEnvVar(this.env, "COLLIBRA_COMMUNITY_DESCRIPTION", false, "Experian description");
    setEnvVar(this.env, "COLLIBRA_USERNAME", true);
    setEnvVar(this.env, "COLLIBRA_PASSWORD", true);
  };

  /**
   * Before getting assets from the source (typically 
   * a data governance center) do whatever clean-up you would like.
   * This might be checking authentication, for example. For Collibra
   * this isn't necessary
   */
  preRetrieveAssets(): PromiseLike<any> {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  };

  /**
   * Get assets from a source. For Collibra this isn't necessary
   */
  retrieveAssets(): PromiseLike<any> {
    const username = this.env.COLLIBRA_USERNAME;
    const password = this.env.COLLIBRA_PASSWORD;
    if (username && password) {
      return this.authenticate(username, password);
    }

    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  };

  /**
   * After getting the assets do whatever clean-up you would like. 
   * This might be sending an email or another type of notification
   * to a system.
   */
  postRetrieveAssets(data: any): PromiseLike<any> {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  };

  /**
   * Before sending the Data Quality Rules do whatever clean-up you
   * would like. This might be checking to see if data quality rules exist
   * before retrieval.
   */
  preSendDataQualityRules(): PromiseLike<any> { 
    return Promise.all([
      this.getOrCreateRelationTypes({
        coRole: "is classified by",
        role: "classifies",
        sourceTypeId: "00000000-0000-0000-0000-000000031108", // Data Quality Dimension
        targetTypeId: "00000000-0000-0000-0000-000000031107", // Data Quality Metric
        saveTo: "DimensionToMetric"
      }),
      this.getOrCreateRelationTypes({
        coRole: "executes",
        role: "executed by",
        sourceTypeId: "00000000-0000-0000-0000-000000031205", // Data Quality Rule
        targetTypeId: "00000000-0000-0000-0000-000000031107", // Data Quality Metric
        saveTo: "RuleToMetric"
      }),
    ]);
  };

  /**
   * Send data quality rules to the system. The rules that are chosen to 
   * be sent will depend on the configuration options you have selected. 
   */
  sendDataQualityRules(): PromiseLike<any>  {
    return this.preSendDataQualityRules().then(() => {
      const communityName = this.env.COLLIBRA_COMMUNITY_NAME;
      const communityDescription = this.env.COLLIBRA_COMMUNITY_DESCRIPTION;

      setEnvVar(this.env, "COLLIBRA_GOVERNANCE_NAME", false, "Data Quality Results");
      setEnvVar(this.env, "COLLIBRA_GOVERNANCE_DESCRIPTION", false, "Experian data quality");

      setEnvVar(this.env, "COLLIBRA_RULEBOOK_NAME", false, "Data Quality Rules");
      setEnvVar(this.env, "COLLIBRA_RULEBOOK_DESCRIPTION", false, "Experian data quality rules");

      const domains = [
        { 
          name: this.env.COLLIBRA_GOVERNANCE_NAME, 
          typeName: "Governance Asset Domain",
          typeId: "00000000-0000-0000-0000-000000030003", 
          description: this.env.COLLIBRA_GOVERNANCE_DESCRIPTION
        },
        { 
          name: this.env.COLLIBRA_RULEBOOK_NAME, 
          typeName: "Rulebook",
          typeId: "00000000-0000-0000-0000-000000030023", 
          description: this.env.COLLIBRA_RULEBOOK_DESCRIPTION
        }
      ];

      let communityId: string; 

      // Create community if non existent
      this.importApi([{
        "resourceType": "Community",
        "identifier": {
          "name": communityName
        },
        "description": communityDescription
      }]).then(() => {
        log("Creating or updating domains...", 4);
        const mappedDomains = domains.map((domain) => {
          return {
            "resourceType": "Domain",
            "identifier": {
              "name": domain.name,
              "community": {
                "name": communityName
              }
            },
            "type": {
              "name": domain.typeName
            },
            "description": domain.description
          }
        });
        return this.importApi(mappedDomains);
      }).then(() => {
        try {
          log("Querying rule data...", 4);
          setEnvVar(this.env, "HTTP_ODBC_RULE_QUERY", true);
        } catch(e) {
          log(JSON.stringify(e), "error");
          return new Promise((resolve) => { resolve(null) });
        }

        return axios.request({ 
          url: `${this.env.HTTP_ODBC_URL}/query`,
          method: "POST",
          headers: { "Content-Type": "application/json" },
          data: {
            sql: this.env.HTTP_ODBC_RULE_QUERY
          }
        })      
      }).then((response: any) => {
        const rules: PandoraRule[] = this.processRuleData(response.data);
        setEnvVar(this.env, "COLLIBRA_DATA_ASSET_NAME", false, "Data Asset Domain");
        setEnvVar(this.env, "COLLIBRA_DATA_ASSET_DESCRIPTION", false, "Assets from database");
        const dataAssetTypeName = this.env.COLLIBRA_DATA_ASSET_NAME;
        const dataAssetDomainDescription = this.env.COLLIBRA_DATA_ASSET_DESCRIPTION;

        // Create Data Asset Domain
        log("Creating or updating asset domain...", 4);

        return new Promise((resolve: any) => {
          rules.forEach((rule: PandoraRule, index: number, array: any) => {
            this.importApi([{
              "resourceType": "Domain",
              "identifier": {
                "name": this.getAttribute(rule, "EXTERNAL SERVER"),
                "community": {
                  "name": communityName
                }
              },
              "type": {
                "name": dataAssetTypeName
              },
              "description": dataAssetDomainDescription
            }]).then(() => {
              return this.importApi([{
                "resourceType": "Asset",
                "identifier": {
                  "name": this.getAttribute(rule, "EXTERNAL DATABASE"),
                  "domain": {
                    "name": this.getAttribute(rule, "EXTERNAL SERVER"),
                    "community": {
                      "name": communityName
                    }
                  }
                },
                "domain": {
                  "name": this.getAttribute(rule, "EXTERNAL SERVER")
                },
                "name": this.getAttribute(rule, "EXTERNAL DATABASE"),
                "displayName": this.getAttribute(rule, "EXTERNAL DATABASE"),
                "type": {
                  "name": "Database"
                }
              }])
            }).then(() => {
              const tableDatabaseRelationTypeId = "00000000-0000-0000-0000-000000007045"; 
              return this.importApi([
                {
                  "resourceType": "Asset",
                  "identifier": { "name": this.getAttribute(rule, "EXTERNAL TABLE NAME"),
                    "domain": {
                      "name": this.getAttribute(rule, "EXTERNAL SERVER"),
                      "community": {
                        "name": communityName
                      }
                    }
                  },
                  "domain": {
                    "name": this.getAttribute(rule, "EXTERNAL SERVER")
                  },
                  "name": this.getAttribute(rule, "EXTERNAL TABLE NAME"),
                  "displayName": this.getAttribute(rule, "EXTERNAL TABLE NAME"),
                  "type": {
                    "name": "Table"
                  },
                  "relations": {
                    "00000000-0000-0000-0000-000000007045:TARGET": [{
                      "name": this.getAttribute(rule, "EXTERNAL DATABASE"),
                      "domain": {
                        "name": this.getAttribute(rule, "EXTERNAL SERVER"),
                        "community": {
                          "name": communityName
                        }
                      }
                    }]
                  },
                  "status": {
                    "name": "Candidate"
                  }
                },
                {
                  "resourceType": "Asset",
                  "identifier": {
                    "name": this.getAttribute(rule, "EXTERNAL COLUMN NAME"),
                    "domain": {
                      "name": this.getAttribute(rule, "EXTERNAL SERVER"),
                      "community": {
                        "name": communityName
                      }
                    }
                  },
                  "domain": {
                    "name": this.getAttribute(rule, "EXTERNAL SERVER")
                  },
                  "name": this.getAttribute(rule, "EXTERNAL COLUMN NAME"),
                  "displayName": this.getAttribute(rule, "EXTERNAL COLUMN NAME"),
                  "type": {
                    "name": "Column"
                  },
                  "relations": {
                    "00000000-0000-0000-0000-000000007042:TARGET": [{
                      "name": this.getAttribute(rule, "EXTERNAL TABLE NAME"),
                      "domain": {
                        "name": this.getAttribute(rule, "EXTERNAL SERVER"),
                        "community": {
                          "name": communityName
                        }
                      }
                    }]
                  },
                  "status": {
                    "name": "Candidate"
                  }
                }
              ])
            }).then(() => {
              let attributes: any = {};
              const thresholdMatch = this.getAttribute(rule, "PASS RANGE").match(/(\d+).*/);
              if (thresholdMatch) {
                attributes["Threshold"] = [{
                  value: Number(thresholdMatch[1])
                }]
              }

              const descriptionMatch = this.getAttribute(rule, "RULE DESCRIPTION");
              if (descriptionMatch) {
                attributes["Description"] = [{
                  value: descriptionMatch
                }]
              }

              const exampleMatch = this.getAttribute(rule, "EXAMPLE");
              if (exampleMatch) {
                attributes["Descriptive Example"] = [{
                  value: exampleMatch
                }]
              }

              attributes["Loaded Rows"] = [{
                value: Number(this.getAttribute(rule, "ROWS CONSIDERED"))
              }];

              attributes["Rows Passed"] = [{
                value: Number(this.getAttribute(rule, "ROWS PASSED"))
              }];

              attributes["Conformity Score"] = [{
                value: Number(this.getAttribute(rule, "ROWS PASSED"))
              }];

              attributes["Rows Failed"] = [{
                value: Number(this.getAttribute(rule, "ROWS FAILED"))
              }];

              attributes["Non Conformity Score"] = [{
                value: Number(this.getAttribute(rule, "ROWS FAILED"))
              }];

              let result = this.getAttribute(rule, "RESULT") === "Green" ? true : false;
              if (!result && thresholdMatch) {
                const passRate = Number(this.getAttribute(rule, "ROWS PASSED")) /
                  Number(this.getAttribute(rule, "ROWS CONSIDERED"))
                result = passRate > Number(thresholdMatch[1]) ? true : false;
              }

              if (result) {
                attributes["Result"] = [{
                  value: result
                }];
              }

              attributes["Passing Fraction"] = [{
                value: Number(this.getAttribute(rule, "ROWS PASSED")) / Number(this.getAttribute(rule, "ROWS CONSIDERED")) * 100
              }];

              const lastValidated = this.getAttribute(rule, "LAST VALIDATED");
              if (lastValidated && lastValidated !== "None") { 
                attributes["Last Sync Date"] = [{
                  value: new Date((this.getAttribute(rule, "LAST VALIDATED") + " UTC").replace(/\d\d:\d\d:\d\d/, "00:00:00")).getTime()
                }]
              }

              let relations: any = {};
              const columnMetricTypeId = "00000000-0000-0000-0000-000000007018";
              relations[`${columnMetricTypeId}:SOURCE`] = [{
                "name": this.getAttribute(rule, "EXTERNAL COLUMN NAME"),
                "domain": {
                  "name": this.getAttribute(rule, "EXTERNAL SERVER"),
                  "community": {
                    "name": communityName
                  }
                }
              }];

              let importArray: any[] = [{
                "resourceType": "Asset",
                "identifier": {
                  "name": this.getAttribute(rule, "NAME"),
                  "domain": {
                    "name": this.env.COLLIBRA_GOVERNANCE_NAME,
                    "community": {
                      "name": communityName
                    }
                  }
                },
                "domain": {
                  "name": this.env.COLLIBRA_GOVERNANCE_NAME,
                  "community": {
                    "name": communityName
                  }
                },
                "name": this.getAttribute(rule, "NAME"),
                "displayName": this.getAttribute(rule, "NAME"),
                "type": {
                  "name": "Data Quality Metric" // TODO: Make configurable
                },
                "status": {
                  "name": "Candidate"
                },
                "attributes": attributes,
              }];

              // Create/update dimensions asset, if specified
              const dimensionMatch = this.getAttribute(rule, "DIMENSION")
              if(dimensionMatch) {
                importArray.push({
                  "resourceType": "Asset",
                  "identifier": {
                    "name": dimensionMatch,
                    "domain": {
                      "name": "Data Quality Dimensions",
                      "community": {
                        "name": "Data Governance Council"
                      }
                    }
                  },
                  "domain": {
                    "name": "Data Quality Dimensions",
                    "community": {
                      "name": "Data Governance Council"
                    }
                  },
                  "name": dimensionMatch,
                  "displayName": dimensionMatch,
                  "type": {
                    "name": "Data Quality Dimension"
                  }
                })

                relations[`${this.customRelations["DimensionToMetric"]}:SOURCE`] = [{
                  "name": dimensionMatch,
                  "domain": {
                    "name": "Data Quality Dimensions",
                    "community": {
                      "name": "Data Governance Council"
                    }
                  }
                }];
              }

              // Create/update rule asset, if specified
              const ruleMatch = this.getAttribute(rule, "RULE");
              if(ruleMatch) {
                importArray.push({
                  "resourceType": "Asset",
                  "identifier": {
                    "name": ruleMatch,
                    "domain": {
                      "name": this.env.COLLIBRA_RULEBOOK_NAME,
                      "community": {
                        "name": communityName,
                      }
                    }
                  },
                  "domain": {
                    "name": this.env.COLLIBRA_RULEBOOK_NAME,
                    "community": {
                      "name": communityName
                    }
                  },
                  "name": ruleMatch,
                  "displayName": ruleMatch,
                  "type": {
                    "name": "Data Quality Rule"
                  }
                })

                relations[`${this.customRelations["RuleToMetric"]}:SOURCE`] = [{
                  "name": ruleMatch,
                  "domain": {
                    "name": this.env.COLLIBRA_RULEBOOK_NAME,
                    "community": {
                      "name": communityName
                    }
                  }
                }];
              }

              const tagsMatch = this.getAttribute(rule, "TAGS");
              if(tagsMatch) {
                importArray[0]["tags"] = tagsMatch.split(",");
              }
              const statusMatch = this.getAttribute(rule, "STATUS");
              if(statusMatch) {
                importArray[0]["status"] = {
                  "name": statusMatch
                }
              }

              importArray[0]["relations"] = relations;
              return this.importApi(importArray)
            })

            if (index == array.length -1 ){
              resolve("Import complete");
            }
          })
        })
      });
    })
  }

  /*
   *
   * After sending the data quality rules, do whatever clean-up 
   * you would like. This might be sending an email or another 
   * type of notification to a system.
   */
  postSendDataQualityRules(): PromiseLike<any> {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  }


  preSendDataQualityProfiles(): PromiseLike<any> {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  }

  sendDataQualityProfiles(): PromiseLike<any> {
    return this.preSendDataQualityProfiles().then(() => {
      const communityName = this.env.COLLIBRA_COMMUNITY_NAME;
      const communityDescription = this.env.COLLIBRA_COMMUNITY_DESCRIPTION;

      try {
        setEnvVar(this.env, "HTTP_ODBC_PROFILE_QUERY", true);
      } catch(e) {
        log(JSON.stringify(e), "error");
        return new Promise((resolve) => { resolve(null) });
      }

      // Create community if non existent
      return this.importApi([{
        "resourceType": "Community",
        "identifier": {
          "name": communityName
        },
        "description": communityDescription
      }]).then(() => {
        try {
          log("Querying profile data...", 4);
          setEnvVar(this.env, "HTTP_ODBC_PROFILE_QUERY", true);
        } catch(e) {
          log(JSON.stringify(e), "error");
          return new Promise((resolve) => { resolve(null) });
        }

        return axios.request({
          url: `${this.env.HTTP_ODBC_URL}/query`,
          method: "POST",
          headers: { "Content-Type": "application/json" },
          data: {
            sql: this.env.HTTP_ODBC_PROFILE_QUERY
          }
        })
      }).then((response: any) => {
        const profiles: PandoraProfile[] = this.processProfileData(response.data);
        setEnvVar(this.env, "COLLIBRA_DATA_ASSET_NAME", false, "Data Asset Domain");
        setEnvVar(this.env, "COLLIBRA_DATA_ASSET_DESCRIPTION", false, "Assets from database");
        const dataAssetTypeName = this.env.COLLIBRA_DATA_ASSET_NAME;
        const dataAssetDomainDescription = this.env.COLLIBRA_DATA_ASSET_DESCRIPTION;

        // Create Data Asset Domain
        log("Creating or updating asset domain...", 4);

        return new Promise((resolve: any, reject: any) => {
          profiles.forEach((profile: PandoraProfile, index: number, array: any) => {
            this.importApi([{
              "resourceType": "Domain",
              "identifier": {
                "name": this.getProfileAttribute(profile, "EXTERNAL SERVER"),
                "community": {
                  "name": communityName
                }
              },
              "type": {
                "name": dataAssetTypeName
              },
              "description": dataAssetDomainDescription
            }]).then(() => {
              this.importApi([{
                "resourceType": "Asset",
                "identifier": {
                  "name": this.getProfileAttribute(profile, "EXTERNAL DATABASE"),
                  "domain": {
                    "name": this.getProfileAttribute(profile, "EXTERNAL SERVER"),
                    "community": {
                      "name": communityName
                    }
                  }
                },
                "domain": {
                  "name": this.getProfileAttribute(profile, "EXTERNAL SERVER")
                },
                "name": this.getProfileAttribute(profile, "EXTERNAL DATABASE"),
                "displayName": this.getProfileAttribute(profile, "EXTERNAL DATABASE"),
                "type": {
                  "name": "Database"
                }
              }])
            }).then(() => {
              const tableDatabaseRelationTypeId = "00000000-0000-0000-0000-000000007045"; 
              return this.importApi([
                {
                  "resourceType": "Asset",
                  "identifier": { 
                    "name": this.getProfileAttribute(profile, "TABLE EXTERNAL NAME"),
                    "domain": {
                      "name": this.getProfileAttribute(profile, "EXTERNAL SERVER"),
                      "community": {
                        "name": communityName
                      }
                    }
                  },
                  "domain": {
                    "name": this.getProfileAttribute(profile, "EXTERNAL SERVER")
                  },
                  "name": this.getProfileAttribute(profile, "TABLE EXTERNAL NAME"),
                  "displayName": this.getProfileAttribute(profile, "TABLE EXTERNAL NAME"),
                  "type": {
                    "name": "Table"
                  },
                  "relations": {
                    "00000000-0000-0000-0000-000000007045:TARGET": [{
                      "name": this.getProfileAttribute(profile, "EXTERNAL DATABASE"),
                      "domain": {
                        "name": this.getProfileAttribute(profile, "EXTERNAL SERVER"),
                        "community": {
                          "name": communityName
                        }
                      }
                    }]
                  },
                  "status": {
                    "name": "Candidate"
                  }
                },
                {
                  "resourceType": "Asset",
                  "identifier": {
                    "name": this.getProfileAttribute(profile, "EXTERNAL NAME"),
                    "domain": {
                      "name": this.getProfileAttribute(profile, "EXTERNAL SERVER"),
                      "community": {
                        "name": communityName
                      }
                    }
                  },
                  "domain": {
                    "name": this.getProfileAttribute(profile, "EXTERNAL SERVER")
                  },
                  "name": this.getProfileAttribute(profile, "EXTERNAL NAME"),
                  "displayName": this.getProfileAttribute(profile, "EXTERNAL NAME"),
                  "type": {
                    "name": "Column"
                  },
                  "relations": {
                    "00000000-0000-0000-0000-000000007042:TARGET": [{
                      "name": this.getProfileAttribute(profile, "TABLE EXTERNAL NAME"),
                      "domain": {
                        "name": this.getProfileAttribute(profile, "EXTERNAL SERVER"),
                        "community": {
                          "name": communityName
                        }
                      }
                    }]
                  },
                  "status": {
                    "name": "Candidate"
                  }
                }
              ])
            }).then(() => {
              let attributes: any = {};
              const rowCount = Number(this.getProfileAttribute(profile, "ROW COUNT"));
              if (rowCount) {
                attributes["Row Count"] = [{
                  value: rowCount
                }];
              }

              const blankCount = Number(this.getProfileAttribute(profile, "BLANK COUNT"));
              if (blankCount) {
                attributes["Empty Values Count"] = [{
                  value: blankCount
                }];
              }

              const nullCount = Number(this.getProfileAttribute(profile, "NULL COUNT"));
              if (!blankCount && nullCount) {
                attributes["Empty Values Count"] = [{
                  value: nullCount
                }];
              }

              const uniqueCount = Number(this.getProfileAttribute(profile, "UNIQUE COUNT"));
              if (uniqueCount) {
                attributes["Number of distinct values"] = [{
                  value: uniqueCount
                }];
              }

              const pandoraToCollibraDataType: any = {
                "Alphanumeric": "Text",
                "Integer": "Whole Number",
                "Decimal": "Decimal Number",
                "Date": "Date Time",
              };

              const dominantDatatype = pandoraToCollibraDataType[this.getProfileAttribute(profile, "DOMINANT DATATYPE")];
              if (dominantDatatype) {
                attributes["Data Type"] = [{
                  value: dominantDatatype
                }];
              }

              const nativeType = this.getProfileAttribute(profile, "NATIVE TYPE");
              if (nativeType) {
                attributes["Technical Data Type"] = [{
                  value: nativeType
                }];
              }

              const standardDeviation = this.getProfileAttribute(profile, "STANDARD DEVIATION OF VALUES")
              if (standardDeviation) {
                attributes["Standard Deviation"] = [{
                  value: standardDeviation
                }];
              }

              const mostCommonValue = this.getProfileAttribute(profile, "MOST COMMON VALUE");
              if (mostCommonValue) {
                attributes["Mode"] = [{
                  value: mostCommonValue
                }];
              }

              const minimumValue = this.getProfileAttribute(profile, "MINIMUM");
              if (minimumValue) {
                attributes["Minimum Value"] = [{
                  value: minimumValue
                }];
              }

              const minimumTextLength = this.getProfileAttribute(profile, "ALPHANUMERIC MIN LENGTH");
              if (minimumTextLength) {
                attributes["Minimum Text Length"] = [{
                  value: minimumTextLength
                }];
              }

              const average = this.getProfileAttribute(profile, "AVERAGE");
              if (average) {
                attributes["Mean"] = [{
                  value: average
                }];
              }

              const maximumValue = this.getProfileAttribute(profile, "MAXIMUM")
              if (maximumValue) {
                attributes["Maximum Value"] = [{
                  value: maximumValue
                }];
              }

              const maximumTextLength = this.getProfileAttribute(profile, "ALPHANUMERIC MAX LENGTH");
              if (maximumTextLength) {
                attributes["Maximum Text Length"] = [{
                  value: maximumTextLength
                }];
              }

              const keyCheck = this.getProfileAttribute(profile, "KEY CHECK") === "Key" ? true : false;
              if (keyCheck) {
                attributes["Is Primary Key"] = [{
                  value: keyCheck
                }];
              }

              const documentedNullable = this.getProfileAttribute(profile, "DOCUMENTED NULLABLE") === "No" ? false : true;
              if (documentedNullable) {
                attributes["Is Nullable"] = [{
                  value: documentedNullable 
                }];
              }

              const position = this.getProfileAttribute(profile, "POSITION")
              if (position) {
                attributes["Column Position"] = [{
                  value: position
                }];
              }

              const originalName = this.getProfileAttribute(profile, "NAME");
              if (originalName) {
                attributes["Original Name"] = [{
                  value: originalName
                }];
              }

              const tableDatabaseRelationTypeId = "00000000-0000-0000-0000-000000007045"; 
              this.importApi([
                {
                  "resourceType": "Asset",
                  "identifier": {
                    "name": this.getProfileAttribute(profile, "TABLE EXTERNAL NAME"),
                    "domain": {
                      "name": this.getProfileAttribute(profile, "EXTERNAL SERVER"),
                      "community": {
                        "name": communityName
                      }
                    }
                  },
                  "domain": {
                    "name": this.getProfileAttribute(profile, "EXTERNAL SERVER")
                  },
                  "name": this.getProfileAttribute(profile, "TABLE EXTERNAL NAME"),
                  "displayName": this.getProfileAttribute(profile, "TABLE EXTERNAL NAME"),
                  "type": {
                    "name": "Table"
                  },
                  "relations": {
                    "00000000-0000-0000-0000-000000007045:TARGET": [{
                      "name": this.getProfileAttribute(profile, "EXTERNAL DATABASE"),
                      "domain": {
                        "name": this.getProfileAttribute(profile,"EXTERNAL SERVER"),
                        "community": {
                          "name": communityName
                        }
                      }
                    }]
                  },
                },
                {
                  "resourceType": "Asset",
                  "identifier": {
                    "name": this.getProfileAttribute(profile, "EXTERNAL NAME"),
                    "domain": {
                      "name": this.getProfileAttribute(profile, "EXTERNAL SERVER"),
                      "community": {
                        "name": communityName
                      }
                    }
                  },
                  "domain": {
                    "name": this.getProfileAttribute(profile, "EXTERNAL SERVER")
                  },
                  "name": this.getProfileAttribute(profile, "EXTERNAL NAME"),
                  "displayName": this.getProfileAttribute(profile, "EXTERNAL NAME"),
                  "type": {
                    "name": "Column"
                  },
                  "attributes": attributes,
                  "relations": {
                    "00000000-0000-0000-0000-000000007042:TARGET": [{
                      "name": this.getProfileAttribute(profile, "TABLE EXTERNAL NAME"),
                      "domain": {
                        "name": this.getProfileAttribute(profile, "EXTERNAL SERVER"),
                        "community": {
                          "name": communityName
                        }
                      }
                    }]
                  }
                }
              ])
            })

            if (index == array.length - 1 ){
              resolve("Import complete");
            }
          })
        })
      });
    })  
  }

  postSendDataQualityProfiles(): PromiseLike<any> {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  }

  /**
   * Profile data between Pandora and Data Studio differ in subtle ways. 
   * This function standardizes stuff to minimize differences when querying.
   *
   * @param responseData Represents the unprocessed data straight from the HTTP-ODBC connector
   * @returns An array of the transformed objects
   */
  processProfileData(responseData: object[]): PandoraProfile[] {
    return <PandoraProfile[]> responseData.map(this.maximizeKeys);
  }

  /**
   * Rule data between Pandora and Data Studio differ in subtle ways. 
   * This function standardizes stuff to minimize differences when querying.
   *
   * @param responseData Represents the unprocessed data straight from the HTTP-ODBC connector
   * @returns An array of the transformed objects
   */
  processRuleData(responseData: object[]): PandoraRule[] {
    return <PandoraRule[]> responseData.map(this.maximizeKeys);
  }


  /**
   * Maximize keys of an object
   *
   * @example 
   * ```javascript
   * maximizeKeys({"maximize": "val"})
   * { "MAXIMIZE": "val" }
   * ```
   *
   * @param obj an object with keys
   * @returns an object with all of its keys maximized
   */
  maximizeKeys(obj: object): object {
    let newObj = JSON.parse(JSON.stringify(obj));
    Object.keys(newObj).forEach((key) => {
      newObj[key.toUpperCase()] = newObj[key];
      if (key.toUpperCase() != key) {
        delete newObj[key];
      }
    });

    return newObj;
  }

  private authenticate(username: string, password: string): PromiseLike<any> {
    return new Promise((resolve: any, reject: any) => {
      axios.request({ 
        url: `${this.env.COLLIBRA_URL}/rest/2.0/auth/sessions`,
        method: "POST",
        headers: { "Content-Type": "application/json" },
        data: {
          username,
          password
        }
      }).then((response) => {
        const data = response.data;
        if (data) {
          log("Sign in successful.", 4);
          this.cookie = response.headers["set-cookie"].join(" ");
          resolve(data);
        }
      }).catch((error) => {
        log("Session already found. Deleting...", 4);
        log(JSON.stringify(error.code));
        log(JSON.stringify(error.response.data));
        axios.request({
          url: `${this.env.COLLIBRA_URL}/rest/2.0/auth/sessions/current`,
          method: "DELETE",
          headers: { "Content-Type": "application/json" }
        }).then(() => {
          log("User logged out. Retrying...", 4);
          resolve(this.authenticate(username, password));
        }).catch((error) => {
          log(JSON.stringify(error.response.data), 2);
          reject(JSON.stringify(error));
        })
      });
    })
  }

  private importApi(args: any): PromiseLike<object> {
    let formData = new FormData();
    let fileData = Buffer.from(JSON.stringify(args));
    formData.append("file", fileData, "file.txt");
    formData.append("batchSize", 4000);
    formData.append("fileName", "file.txt");

    // @ts-ignore
    let formDataBuffer = formData.getBuffer();
    let headers = Object.assign(formData.getHeaders(), { "Cookie": this.cookie });

    return new Promise((resolve, reject) => {
      log(`Starting import now with arguments: ${JSON.stringify(args)}`, 5);
      axios.request({ 
        url: `${this.env.COLLIBRA_URL}/rest/2.0/import/json-job`,
        method: "POST",
        headers,
        data: formDataBuffer
      }).then((response) => {
        const jobId = response.data.id;
        ids[jobId] = JSON.stringify(args)
        log(`Import started. Job Id: ${jobId}`, 4);
        let cont = setInterval(() => {
          axios.request({
            method: "GET",
            url: `${this.env.COLLIBRA_URL}/rest/2.0/jobs/${jobId}`,
            headers: { "Content-Type": "application/json", "Cookie": this.cookie },
          }).then(({ data }) => {
            if (data.state === "ERROR") {
              clearInterval(cont);
              log(`Import with Job Id: ${data.id} failed.`, 5);
              log(`Values for Id: ${data.id} - ${ids[data.id]}`, 5);
              reject(JSON.stringify(data));
            }
            else if (data.state === "COMPLETED") {
              clearInterval(cont);
              log(`Import successfully completed.`, 5);
              resolve(data);
            } else {
              clearInterval(cont);
              log(`Import completed with warnings.`, 5);
              resolve({ error: "Something strange happened. Please try again"});
            }
          })
        }, 1000)
      }).catch((err) => {
        log(`Import not successful.`, 5);
        log(JSON.stringify(err.response.data), 2);
        reject(JSON.stringify(err));
      })
    });
  }

  private getOrCreateRelationTypes(args: any): PromiseLike<any> {
    const { coRole, role, sourceTypeId, targetTypeId, saveTo } = args;

    if (!coRole) { 
      throw "RelationTypeError: Something went wrong. A corole is not specified.";
    }

    if (!role) {
      throw "RelationTypeError: Something went wrong. A role is not specified.";
    }

    if (this["customRelations"][saveTo]) {
      return new Promise((resolve) => {
        resolve(this["customRelations"][saveTo]);
      })
    }

    /* In effect, this checks to see if the RelationType exists in the domain
     * If it does exist, we take its value and patch it, continuing.
     * If it does not exist we create it and save its value, continuing.
     */
    return new Promise((resolve: any, reject: any) => {
      axios.request({ 
        url: `${this.env.COLLIBRA_URL}/rest/2.0/relationTypes`,
        method: "GET",
        headers: { "Content-Type": "application/json", "Cookie": this.cookie },
        params: {
          coRole,
          role,
          sourceTypeId,
          targetTypeId 
        }
      }).then((response) => {
        const data = response.data;

        if (data.total === 0) {
          log(`No relationType with name '${name}' found. Creating now...`, 5);
          axios.request({ 
            url: `${this.env.COLLIBRA_URL}/rest/2.0/relationTypes`,
            method: "POST",
            headers: { "Content-Type": "application/json", "Cookie": this.cookie },
            data: {
              coRole,
              role,
              sourceTypeId, 
              targetTypeId,
            }
          }).then((response: any) => {
            const relationType = response.data;
            log(`RelationType with ID ${relationType.id} created successfully`, 5);
            this["customRelations"][saveTo] = relationType.id;
            resolve(this.getOrCreateRelationTypes({coRole, role, sourceTypeId, targetTypeId, saveTo}))
          }).catch((error: any) => {
            log(`PostAssetError with parameters: ${JSON.stringify(args)}`, 2);
            log(JSON.stringify(error.response.data), 2);
            reject(JSON.stringify(error));
          })

        } else {
          this["customRelations"][saveTo] = data.results[0].id;
          log(`Id (${data.results[0].id}) for relation type saved. Continuing...`, 5);
          resolve(this.getOrCreateRelationTypes({coRole, role, sourceTypeId, targetTypeId, saveTo}))
        }
      }).catch((error: any) => {
        log(`RelationTypeError with parameters: ${JSON.stringify(args)}`, 2);
        log(JSON.stringify(error.response.data), 2);
        reject(JSON.stringify(error));
      })
    }); 
  }

  // The main purpose of this function is to provide a fall-back for getting attributes when they
  // don't exist within the specified column name name
  private getAttribute(rule: any, key: string): string {
    if (rule[key]) {
      // Necessary for using multiple communities
      if (process.env.COLLIBRA_MULTI_COMMUNITY && key === "EXTERNAL SERVER") {
        return `${process.env.COLLIBRA_COMMUNITY_NAME}.${rule[key]}`;
      }
      return rule[key];
    } else {
      let attributeKey = rule["DESCRIPTION"];

      try {
        if (process.env.COLLIBRA_ATTRIBUTE_KEY) {
          attributeKey = rule[process.env.COLLIBRA_ATTRIBUTE_KEY];
        }

        const matchedValue: string = attributeKey.match(new RegExp(key + "=([^;]*)"))[1];
        log(`Matched value is: ${matchedValue} for key ${key}`, "debug");
        return matchedValue;
      } catch(e) {
        log(e, "info");
        log(`Value was: ${JSON.stringify(rule[attributeKey])} with key: ${key}`, "error");
        return "";
      }
    }
  }

  private getProfileAttribute(profile: any, key: string): string {
    try {
      // Necessary for using multiple communities
      if (process.env.COLLIBRA_MULTI_COMMUNITY && key === "EXTERNAL SERVER") {
        return `${process.env.COLLIBRA_COMMUNITY_NAME}.${profile[key]}`;
      }

      return profile[key];
    } catch(e) {
      log(JSON.stringify(e), "error");
      return "";
    }
  }
};

const runner = new CollibraConnector({});
runner.retrieveAssets().then(() => {
  runner.sendDataQualityRules();
  runner.sendDataQualityProfiles();
});
