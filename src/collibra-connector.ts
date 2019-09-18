import Connector from "./connector";
//@ts-ignore
import jsonFile from "edit-json-file";
import axios from "axios";
import * as EventEmitter from "events";
import produce from "immer";
import Promise from "bluebird";
import path from "path";
import FormData from "form-data";
import * as parseArgs from "minimist";

const debug = true;

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
function log(message: string, level: number = 1): void {
  /* Levels
   * 0: No logging
   * 1: FATAL
   * 2: ERROR
   * 3: WARNING
   * 4: INFO
   * 5: DEBUG
   */
  const debugLevel = Number(process.env.COLLIBRA_DEBUG_LEVEL) || 3;
  if (debug && debugLevel > level) {
    console.log(message);
  }
}

interface PandoraRule {
    "NAME": string;
    "SCORE": string;
    "MEASURE": string;
    "PASSED MEASURE": string;
    "FAILED MEASURE": string;
    "RULE CATEGORY": string;
    "DESCRIPTION": string;
    "INCLUDED": string;
    "FILTER": string;
    "ROWS PASSED": string;
    "ROWS FAILED": string;
    "TABLE": string;
    "TABLE VERSION": string;
    "VERSIONS OFFSET": string;
    "COLUMN": string;
    "SCHEMA": string;
    "FUNCTION": string;
    "VERSION": string;
    "PARAMETERS": string;
    "TYPE": string;
    "ROWS CONSIDERED": string;
    "ROWS IGNORED": string;
    "PASSED": string;
    "FAILED": string;
    "CONSIDERED": string;
    "IGNORED": string;
    "CATEGORY": string;
    "TABLE CREATED": string;
    "LAST VALIDATED": string;
    "CONNECTION STRING": string;
    "EXTERNAL COLUMN NAME": string;
    "EXTERNAL DATABASE": string;
    "EXTERNAL SCHEMA": string;
    "EXTERNAL SERVER": string;
    "EXTERNAL TABLE NAME": string;
    "FAIL RANGE": string;
    "FAILED SCORE": string;
    "LATEST TABLE VERSION": string;
    "PASS RANGE": string;
    "RESULT": string;
    "RULE CATEGORY ID": string;
    "RULE THRESHOLD": string;
}

interface PandoraProfile {
  "ROW COUNT": string;
  "BLANK COUNT": string;
  "UNIQUE COUNT": string;
  "DOMINANT DATATYPE": string;
  "NATIVE TYPE": string;
  "STANDARD DEVIATION OF VALUES": string;
  "MOST COMMON VALUE": string;
  "MINIMUM": string;
  "ALPHANUMERIC MIN LENGTH": string;
  "AVERAGE": string;
  "MAXIMUM": string;
  "ALPHANUMERIC MAX LENGTH": string;
  "KEY CHECK": string;
  "DOCUMENTED NULLABLE": string;
  "POSITION": string;
  "NAME": string;
  "EXTERNAL NAME": string; // Equivalent to External Column Name in PandoraRule
  "EXTERNAL DATABASE": string;
  "EXTERNAL SCHEMA": string;
  "EXTERNAL SERVER": string;
  "TABLE EXTERNAL NAME": string; // Equivalent to External Table Name
}

interface ConstructorArgs {
  /** URL of the ODBC endpoint */
  odbcUrl?: string;
}

export class CollibraConnector extends Connector {

  // File object that contains config
  configuration: any;

  // Environment location of the collibra environment, e.g. https://your-env-dev.collibra.com/
  url?: string;

  // Date of the last run
  lastRun: Date;

  // Location to the HTTP ODBC API
  odbcUrl: string | undefined


  // Used to make subsequent API requests to the Collibra Data Governance Center API.
  cookie: string;

  // Custom relation types
  customRelations: any; 

  /**
   * The name of the community as it will appear in Collibra
   */
  communityName: string;

  /**
   * The description of the community as it will be described with 
   * the communityName
   */
  communityDescription: string;

  constructor(args: ConstructorArgs) {
    super();

    let odbcUrl = this.odbcUrl;
    this.configuration = jsonFile(`${path.resolve(__dirname, "..")}/connector-config.json`);

    // const URL = this.configuration.get("CollibraConnector.collibraUrl");
    const URL = process.env.COLLIBRA_URL;
    if (!URL) {
      throw "COLLIBRA_URL not found";
    } else {
      this.url = URL;
    }

    // const LAST_RUN = this.configuration.get("CollibraConnector.lastRun");
    const LAST_RUN = process.env.COLLIBRA_LAST_RUN;
    if (!LAST_RUN) {
      log("lastRun not found", 4)
      log("Defaulting to 1900-01-01", 4);
      this.lastRun = new Date("1900-01-01");
    } else {
      this.lastRun = new Date(LAST_RUN);
    }

    const HTTP_ODBC_URL = process.env.HTTP_ODBC_URL;
    if (!HTTP_ODBC_URL && !odbcUrl) {
      throw("Please set HTTP_ODBC_URL or pass in odbcUrl into the command line.");
    } else if (odbcUrl){
      this.odbcUrl = odbcUrl;
    } else {
      this.odbcUrl = HTTP_ODBC_URL;
    }
    this.cookie = "";
    this.customRelations = {};

    const configuredCollibraName = process.env["COLLIBRA_COMMUNITY_NAME"];
    if (!configuredCollibraName) {
      log(`You don't have a name set (COLLIBRA_COMMUNITY_NAME) for the community name, defaulting
          to 'javascript community'`, 3); }

    this.communityName = configuredCollibraName || "javascript community";

    const configuredCollibraDescription = process.env["COLLIBRA_COMMUNITY_DESCRIPTION"];
    if (!configuredCollibraDescription) {
      log(`You don't have a description set (COLLIBRA_COMMUNITY_DESCRIPTION) for the community
          description , defaulting to 'community description'`, 3); }

    this.communityDescription = configuredCollibraDescription || "community description";

  };

  /*
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

  /*
   * Get assets from a source. For Collibra this isn't necessary
   */
  retrieveAssets(): PromiseLike<any> {
    // TODO: Move to config
    const username = process.env["COLLIBRA_ADMIN"] || "Admin";
    const password = process.env["COLLIBRA_PASSWORD"] || "Password123";
    return this.authenticate(username, password);
  };

  /*
   * After getting the assets do whatever clean-up you would like. 
   * This might be sending an email or another type of notification
   * to a system.
   */
  postRetrieveAssets(data: any): PromiseLike<any> {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  };

  /*
   * Before sending the Data Quality Rules do whatever clean-up you
   * would like. This might be checking to see if data quality rules exist
   * before retrieval.
   */
  preSendDataQualityRules(): PromiseLike<any> { 
    return Promise.all([
      this.getOrCreateRelationTypes({
        coRole: "is classified by",
        role: "classifies",
        sourceTypeId: "00000000-0000-0000-0000-000000031108",
        targetTypeId: "00000000-0000-0000-0000-000000031107",
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

  /*
   * Send data quality rules to the system. The rules that are chosen to 
   * be sent will depend on the configuration options you have selected. 
   */
  sendDataQualityRules(): PromiseLike<any>  {
    return this.preSendDataQualityRules().then(() => {
      const communityName = this.communityName;
      const communityDescription = this.communityDescription;

      const domains = [
        { 
          name: "Data Quality Results", 
          typeName: "Governance Asset Domain",
          typeId: "00000000-0000-0000-0000-000000030003", 
          description: "Data quality imported from Experian" 
        },
        { 
          name: "Data Quality Rules", 
          typeName: "Rulebook",
          typeId: "00000000-0000-0000-0000-000000030023", 
          description: "Data quality rules" 
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
        log("Querying rules...", 4);
        return axios.request({ 
          url: `${this.odbcUrl}/query`,
          method: "POST",
          headers: { "Content-Type": "application/json" },
          data: {
            sql: `SELECT * FROM \"RULES\"`,
          }
        })      
      }).then((response: any) => {
        const rules: PandoraRule[] = this.processRuleData(response.data);

        // Configuration
        const dataAssetDomainDescription = "Assets from database"; 
        const dataAssetTypeName = "Data Asset Domain"; 
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
                  "identifier": {
                    "name": this.getAttribute(rule, "EXTERNAL TABLE NAME"),
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
                  value: descriptionMatch[1]
                }]
              }

              const exampleMatch = this.getAttribute(rule, "EXAMPLE");
              if (exampleMatch) {
                attributes["Descriptive Example"] = [{
                  value: exampleMatch[1]
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

              attributes["Result"] = [{
                value: this.getAttribute(rule, "RESULT") === "Green" ? true : false
              }];

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
                  "name": this.getAttribute(rule, "EXTERNAL SERVER"), // TODO: Make configurable
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
                    "name": "Data Quality Results", // TODO: Make configurable
                    "community": {
                      "name": communityName
                    }
                  }
                },
                "domain": {
                  "name": "Data Quality Results", // TODO: Make configurable
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
              const dimensionMatch = this.getAttribute(rule, "dimension")
              if(dimensionMatch && dimensionMatch.length === 2) {
                importArray.push({
                  "resourceType": "Asset",
                  "identifier": {
                    "name": dimensionMatch[1],
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
                  "name": dimensionMatch[1],
                  "displayName": dimensionMatch[1],
                  "type": {
                    "name": "Data Quality Dimension"
                  }
                })

                relations[`${this.customRelations["DimensionToMetric"]}:SOURCE`] = [{
                  "name": dimensionMatch[1],
                  "domain": {
                    "name": "Data Quality Dimensions",
                    "community": {
                      "name": "Data Governance Council"
                    }
                  }
                }];
              }

              // Create/update rule asset, if specified
              const ruleMatch = this.getAttribute(rule, "rule");
              if(ruleMatch && ruleMatch.length === 2) {
                importArray.push({
                  "resourceType": "Asset",
                  "identifier": {
                    "name": ruleMatch[1],
                    "domain": {
                      "name": "Data Quality Rules",
                      "community": {
                        "name": communityName,
                      }
                    }
                  },
                  "domain": {
                    "name": "Data Quality Rules",
                    "community": {
                      "name": communityName
                    }
                  },
                  "name": ruleMatch[1],
                  "displayName": ruleMatch[1],
                  "type": {
                    "name": "Data Quality Rule"
                  }
                })

                relations[`${this.customRelations["RuleToMetric"]}:SOURCE`] = [{
                  "name": ruleMatch[1],
                  "domain": {
                    "name": "Data Quality Rules",
                    "community": {
                      "name": communityName
                    }
                  }
                }];

              }

              const tagsMatch = this.getAttribute(rule, "tags");
              if(tagsMatch && tagsMatch.length === 2) {
                importArray[0]["tags"] = tagsMatch[1].split(",");
              }
              const statusMatch = this.getAttribute(rule, "status");
              if(statusMatch && statusMatch.length === 2) {
                importArray[0]["status"] = {
                  "name": statusMatch[0]
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
      const communityName: string = this.communityName;
      const communityDescription: string = this.communityDescription;

      return this.importApi([{
        "resourceType": "Community",
        "identifier": {
          "name": communityName
        },
        "description": communityDescription
      }]).then(() => {
        return axios.request({
          url: `${this.odbcUrl}/query`,
          method: "POST",
          headers: { "Content-Type": "application/json" },
          data: {
            // TODO: Make the query semi-configurable
            sql: `SELECT * FROM \"SNAPSHOT_01\"`
          }
        })
      }).then((response: any) => {
        const profiles: PandoraProfile[] = this.processProfileData(response.data);

        // Configuration
        const dataAssetDomainDescription = "Assets from database"; 
        const dataAssetTypeName = "Data Asset Domain"; 
        log("Creating or updating asset domain...", 5);

        return new Promise((resolve: any, reject: any) => {
          profiles.forEach((profile: PandoraProfile, index: number, array: any) => {
            this.importApi([{
              "resourceType": "Domain",
              "identifier": {
                "name": this.getAttribute(profile, "EXTERNAL SERVER"),
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
              let attributes: any = {};
              attributes["Row Count"] = [{
                value: Number(this.getProfileAttribute(profile, "ROW COUNT"))
              }];

              attributes["Empty Values Count"] = [{
                value: Number(this.getProfileAttribute(profile, "BLANK COUNT"))
              }];

              attributes["Number of distinct values"] = [{
                value: Number(this.getProfileAttribute(profile, "UNIQUE COUNT"))
              }];

              const pandoraToCollibraDataType: any = {
                "Alphanumeric": "Text",
                "Integer": "Number",
                "Decimal": "Decimal",
                "Date": "DateTime"
              };

              attributes["Data Type"] = [{
                value: pandoraToCollibraDataType[this.getProfileAttribute(profile, "DOMINANT DATATYPE")]
              }];

              attributes["Technical Data Type"] = [{
                value: this.getProfileAttribute(profile, "NATIVE TYPE")
              }];

              attributes["Standard Deviation"] = [{
                value: this.getProfileAttribute(profile, "STANDARD DEVIATION OF VALUES")
              }];

              attributes["Mode"] = [{
                value: this.getProfileAttribute(profile, "MOST COMMON VALUE")
              }];

              attributes["Minimum Value"] = [{
                value: this.getProfileAttribute(profile, "MINIMUM")
              }];

              attributes["Minimum Text Length"] = [{
                value: this.getProfileAttribute(profile, "ALPHANUMERIC MIN LENGTH")
              }];

              attributes["Mean"] = [{
                value: this.getProfileAttribute(profile, "AVERAGE")
              }];

              attributes["Maximum Value"] = [{
                value: this.getProfileAttribute(profile, "MAXIMUM")
              }];

              attributes["Maximum Text Length"] = [{
                value: this.getProfileAttribute(profile, "ALPHANUMERIC MAX LENGTH")
              }];

              attributes["Is Primary Key"] = [{
                value: this.getProfileAttribute(profile, "KEY CHECK") === "Key" ? true : false
              }];

              attributes["Is Nullable"] = [{
                value: this.getProfileAttribute(profile, "DOCUMENTED NULLABLE") === "No" ? false : true
              }];

              attributes["Column Position"] = [{
                value: this.getProfileAttribute(profile, "POSITION")
              }];

              attributes["Original Name"] = [{
                value: this.getProfileAttribute(profile, "NAME")
              }];

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
        url: `${this.url}/rest/2.0/auth/sessions`,
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
        log(error.code);
        log(error.response.data);
        axios.request({
          url: `${this.url}/rest/2.0/auth/sessions/current`,
          method: "DELETE",
          headers: { "Content-Type": "application/json" }
        }).then(() => {
          log("User logged out. Retrying...", 4);
          resolve(this.authenticate(username, password));
        }).catch((error) => {
          log(error.response.data, 2);
          reject(error);
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
        url: `${this.url}/rest/2.0/import/json-job`,
        method: "POST",
        headers,
        data: formDataBuffer
      }).then((response) => {
        const jobId = response.data.id;
        log(`Import started. Job Id: ${jobId}`, 4);
        let cont = setInterval(() => {
          axios.request({
            method: "GET",
            url: `${this.url}/rest/2.0/jobs/${jobId}`,
            headers: { "Content-Type": "application/json", "Cookie": this.cookie },
          }).then(({ data }) => {
            if (data.state === "ERROR") {
              clearInterval(cont);
              log(`Import failed.`, 5);
              reject(data);
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
        log(err.response.data, 2);
        reject(err);
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
        url: `${this.url}/rest/2.0/relationTypes`,
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
            url: `${this.url}/rest/2.0/relationTypes`,
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
            log(error.response.data, 2);
            reject(error);
          })

        } else {
          this["customRelations"][saveTo] = data.results[0].id;
          log(`Id (${data.results[0].id}) for relation type saved. Continuing...`, 5);
          resolve(this.getOrCreateRelationTypes({coRole, role, sourceTypeId, targetTypeId, saveTo}))
        }
      }).catch((error: any) => {
        log(`RelationTypeError with parameters: ${JSON.stringify(args)}`, 2);
        log(error.response.data, 2);
        reject(error);
      })
    }); 
  }

  // The main purpose of this function is to provide a fall-back for getting attributes when they
  // don't exist within the specified column name name
  private getAttribute(rule: any, key: string): string {
    if (rule[key]) {
      return rule[key];
    } else {
      try {
        return rule["Rule Name"].match(new RegExp(key + "=([^;]*)"))[1]
      } catch(e) {
        return "";
      }
    }
  }

  private getProfileAttribute(profile: any, key: string): string {
    try {
      return profile[key];
    } catch(e) {
      return "";
    }
  }
};

const runner = new CollibraConnector({});
runner.retrieveAssets().then(() => {
  runner.sendDataQualityProfiles();
});
