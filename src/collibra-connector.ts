import Connector from "./connector";
//@ts-ignore
import jsonFile from "edit-json-file";
import axios from "axios";
import * as EventEmitter from "events";
import produce from "immer";
import Promise from "bluebird";
import path from "path";
import FormData from "form-data";

const debug = true;

function log(message: string) {
  if (debug) {
    console.log(message);
  }
}

interface PandoraRule {
    "NAME": string,
    "SCORE": string,
    "MEASURE": string,
    "PASSED MEASURE": string,
    "FAILED MEASURE": string,
    "RULE CATEGORY": string,
    "DESCRIPTION": string,
    "INCLUDED": string,
    "FILTER": string,
    "ROWS PASSED": string,
    "ROWS FAILED": string,
    "TABLE": string,
    "TABLE VERSION": string,
    "VERSIONS OFFSET": string,
    "COLUMN": string,
    "SCHEMA": string,
    "FUNCTION": string,
    "VERSION": string,
    "PARAMETERS": string,
    "TYPE": string,
    "ROWS CONSIDERED": string,
    "ROWS IGNORED": string,
    "PASSED": string,
    "FAILED": string,
    "CONSIDERED": string,
    "IGNORED": string,
    "CATEGORY": string,
    "TABLE CREATED": string,
    "LAST VALIDATED": string,
    "CONNECTION STRING": string,
    "EXTERNAL COLUMN NAME": string,
    "EXTERNAL DATABASE": string,
    "EXTERNAL SCHEMA": string,
    "EXTERNAL SERVER": string,
    "EXTERNAL TABLE NAME": string,
    "FAIL RANGE": string,
    "FAILED SCORE": string,
    "LATEST TABLE VERSION": string,
    "PASS RANGE": string,
    "RESULT": string,
    "RULE CATEGORY ID": string,
    "RULE THRESHOLD": string
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

export class CollibraConnector extends Connector {

  // File object that contains config
  configuration: any;

  // Environment location of the collibra environment, e.g. https://experian-dev-54.collibra.com/
  url?: string;

  // Date of the last run
  lastRun: Date;

  // Location to the HTTP ODBC API
  odbcUrl: string


  // Used to make subsequent API requests to the Collibra Data Governance Center API.
  cookie: string;

  // Custom relation types
  customRelations: any; 

  constructor() {
    super();

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
      log("lastRun not found")
      log("Defaulting to 1900-01-01");
      this.lastRun = new Date("1900-01-01");
    } else {
      this.lastRun = new Date(LAST_RUN);
    }

    const HTTP_ODBC_URL = process.env.HTTP_ODBC_URL;
    if (!HTTP_ODBC_URL) {
      throw("HTTP_ODBC_URL not found");
    } else {
      this.odbcUrl = HTTP_ODBC_URL;
    }
    this.cookie = "";
    this.customRelations = {};
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
    const username = "Admin";
    const password = "Password123";
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
      // TODO: Move to environment variables or configuration file
      const communityName = "javascript community";
      const communityDescription = "community description";

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
        log("Creating or updating domains...");
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
        log("Querying rules...");
        return axios.request({ 
          url: `${this.odbcUrl}/query`,
          method: "POST",
          headers: { "Content-Type": "application/json" },
          data: {
            sql: `SELECT * FROM \"RULES\" WHERE \"VERSIONS OFFSET\" = 0`,
          }
        })      
      }).then((response: any) => {
        const rules: PandoraRule[] = response.data;

        // Configuration
        const dataAssetDomainDescription = "Assets from database"; 
        const dataAssetTypeName = "Data Asset Domain"; 
        // Create Data Asset Domain
        log("Creating or updating asset domain...");
        
        rules.forEach((rule: PandoraRule) => {
          this.importApi([{
            "resourceType": "Domain",
            "identifier": {
              "name": rule["EXTERNAL SERVER"],
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
                "name": rule["EXTERNAL DATABASE"],
                "domain": {
                  "name": rule["EXTERNAL SERVER"],
                  "community": {
                    "name": communityName
                  }
                }
              },
              "domain": {
                "name": rule["EXTERNAL SERVER"]
              },
              "name": rule["EXTERNAL DATABASE"],
              "displayName": rule["EXTERNAL DATABASE"],
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
                  "name": rule["EXTERNAL TABLE NAME"],
                  "domain": {
                    "name": rule["EXTERNAL SERVER"],
                    "community": {
                      "name": communityName
                    }
                  }
                },
                "domain": {
                  "name": rule["EXTERNAL SERVER"]
                },
                "name": rule["EXTERNAL TABLE NAME"],
                "displayName": rule["EXTERNAL TABLE NAME"],
                "type": {
                  "name": "Table"
                },
                "relations": {
                  "00000000-0000-0000-0000-000000007045:TARGET": [{
                    "name": rule["EXTERNAL DATABASE"],
                    "domain": {
                      "name": rule["EXTERNAL SERVER"],
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
                  "name": rule["EXTERNAL COLUMN NAME"],
                  "domain": {
                    "name": rule["EXTERNAL SERVER"],
                    "community": {
                      "name": communityName
                    }
                  }
                },
                "domain": {
                  "name": rule["EXTERNAL SERVER"]
                },
                "name": rule["EXTERNAL COLUMN NAME"],
                "displayName": rule["EXTERNAL COLUMN NAME"],
                "type": {
                  "name": "Column"
                },
                "relations": {
                  "00000000-0000-0000-0000-000000007042:TARGET": [{
                    "name": rule["EXTERNAL TABLE NAME"],
                    "domain": {
                      "name": rule["EXTERNAL SERVER"],
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
            const thresholdMatch = rule["PASS RANGE"].match(/(\d+).*/);
            if (thresholdMatch) {
              attributes["Threshold"] = [{
                value: Number(thresholdMatch[1])
              }]
            }

            const descriptionMatch = rule["DESCRIPTION"].match(/description=([^;]*)/);
            if (descriptionMatch) {
              attributes["Description"] = [{
                value: descriptionMatch[1]
              }]
            }

            const exampleMatch = rule["DESCRIPTION"].match(/example=([^;]*)/)
            if (exampleMatch) {
              attributes["Descriptive Example"] = [{
                value: exampleMatch[1]
              }]
            }

            attributes["Loaded Rows"] = [{
              value: Number(rule["ROWS CONSIDERED"])
            }];

            attributes["Rows Passed"] = [{
              value: Number(rule["ROWS PASSED"])
            }];

            attributes["Conformity Score"] = [{
              value: Number(rule["ROWS PASSED"])
            }];

            attributes["Rows Failed"] = [{
              value: Number(rule["ROWS FAILED"])
            }];

            attributes["Non Conformity Score"] = [{
              value: Number(rule["ROWS FAILED"])
            }];

            attributes["Result"] = [{
              value: rule["RESULT"] === "Green" ? true : false
            }];

            attributes["Passing Fraction"] = [{
              value: (Number(rule["ROWS PASSED"]) / Number(rule["ROWS CONSIDERED"])) * 100
            }];

            if (rule["LAST VALIDATED"] && rule["LAST VALIDATED"] !== "None") { 
              attributes["Last Sync Date"] = [{
                value: new Date((rule["LAST VALIDATED"] + " UTC").replace(/\d\d:\d\d:\d\d/, "00:00:00")).getTime()
              }]
            }

            let relations: any = {};
            const columnMetricTypeId = "00000000-0000-0000-0000-000000007018";
            relations[`${columnMetricTypeId}:SOURCE`] = [{
              "name": rule["EXTERNAL COLUMN NAME"],
              "domain": {
                "name": rule["EXTERNAL SERVER"], // TODO: Make configurable
                "community": {
                  "name": communityName
                }
              }
            }];

            let importArray: any[] = [{
              "resourceType": "Asset",
              "identifier": {
                "name": rule["NAME"],
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
              "name": rule["NAME"],
              "displayName": rule["NAME"],
              "type": {
                "name": "Data Quality Metric" // TODO: Make configurable
              },
              "status": {
                "name": "Candidate"
              },
              "attributes": attributes,
            }];

            // Create/update dimensions asset, if specified
            const dimensionMatch = rule["DESCRIPTION"].match(/dimension=([^;]*)/); 
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
            const ruleMatch = rule["DESCRIPTION"].match(/rule=([^;]*)/); 
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

            const tagsMatch = rule["DESCRIPTION"].match(/tags=([^;]*)/); 
            if(tagsMatch && tagsMatch.length === 2) {
              importArray[0]["tags"] = tagsMatch[1].split(",");
            }
            const statusMatch = rule["DESCRIPTION"].match(/status=([^;]*)/); 
            if(statusMatch && statusMatch.length === 2) {
              importArray[0]["status"] = {
                "name": statusMatch[0]
              }
            }

            importArray[0]["relations"] = relations;
            return this.importApi(importArray)
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
      // TODO: Move to environment variables or configuration file
      const communityName = "javascript community";
      const communityDescription = "community description";

      // Create community if non existent
      this.importApi([{
        "resourceType": "Community",
        "identifier": {
          "name": communityName
        },
        "description": communityDescription
      }]).then(() => {
        log("Querying rules...");
        return axios.request({
          url: `${this.odbcUrl}/query`,
          method: "POST",
          headers: { "Content-Type": "application/json" },
          data: {
            sql: `SELECT * FROM \"PROFILES\"`
          }
        })
      }).then((response: any) => {
        const rules: PandoraProfile[] = response.data;

        // Configuration
        const dataAssetDomainDescription = "Assets from database"; 
        const dataAssetTypeName = "Data Asset Domain"; 
        // Create Data Asset Domain
        log("Creating or updating asset domain...");
        
        rules.forEach((rule: PandoraProfile) => {
          this.importApi([{
            "resourceType": "Domain",
            "identifier": {
              "name": rule["EXTERNAL SERVER"],
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
                "name": rule["EXTERNAL DATABASE"],
                "domain": {
                  "name": rule["EXTERNAL SERVER"],
                  "community": {
                    "name": communityName
                  }
                }
              },
              "domain": {
                "name": rule["EXTERNAL SERVER"]
              },
              "name": rule["EXTERNAL DATABASE"],
              "displayName": rule["EXTERNAL DATABASE"],
              "type": {
                "name": "Database"
              }
            }])
          }).then(() => {
            let attributes: any = {};
            attributes["Row Count"] = [{
              value: Number(rule["ROW COUNT"])
            }];

            attributes["Empty Values Count"] = [{
              value: Number(rule["BLANK COUNT"])
            }];

            attributes["Number of distinct values"] = [{
              value: Number(rule["UNIQUE COUNT"])
            }];

            const pandoraToCollibraDataType: any = {
              "Alphanumeric": "Text",
              "Integer": "Number",
              "Decimal": "Decimal",
              "Date": "DateTime"
            };

            attributes["Data Type"] = [{
              value: pandoraToCollibraDataType[rule["DOMINANT DATATYPE"]]
            }];

            attributes["Technical Data Type"] = [{
              value: rule["NATIVE TYPE"]
            }];

            attributes["Standard Deviation"] = [{
              value: rule["STANDARD DEVIATION OF VALUES"]
            }];

            attributes["Mode"] = [{
              value: rule["MOST COMMON VALUE"]
            }];

            attributes["Minimum Value"] = [{
              value: rule["MINIMUM"]
            }];

            attributes["Minimum Text Length"] = [{
              value: rule["ALPHANUMERIC MIN LENGTH"]
            }];

            attributes["Mean"] = [{
              value: rule["AVERAGE"]
            }];

            attributes["Maximum Value"] = [{
              value: rule["MAXIMUM"]
            }];

            attributes["Maximum Text Length"] = [{
              value: rule["ALPHANUMERIC MAX LENGTH"]
            }];

            attributes["Is Primary Key"] = [{
              value: rule["KEY CHECK"] === "Key" ? true : false
            }];

            attributes["Is Nullable"] = [{
              value: rule["DOCUMENTED NULLABLE"] === "No" ? false : true
            }];

            attributes["Column Position"] = [{
              value: rule["POSITION"]
            }];

            attributes["Original Name"] = [{
              value: rule["NAME"]
            }];

            const tableDatabaseRelationTypeId = "00000000-0000-0000-0000-000000007045"; 
            return this.importApi([
              {
                "resourceType": "Asset",
                "identifier": {
                  "name": rule["TABLE EXTERNAL NAME"],
                  "domain": {
                    "name": rule["EXTERNAL SERVER"],
                    "community": {
                      "name": communityName
                    }
                  }
                },
                "domain": {
                  "name": rule["EXTERNAL SERVER"]
                },
                "name": rule["TABLE EXTERNAL NAME"],
                "displayName": rule["TABLE EXTERNAL NAME"],
                "type": {
                  "name": "Table"
                },
                "relations": {
                  "00000000-0000-0000-0000-000000007045:TARGET": [{
                    "name": rule["EXTERNAL DATABASE"],
                    "domain": {
                      "name": rule["EXTERNAL SERVER"],
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
                  "name": rule["EXTERNAL NAME"],
                  "domain": {
                    "name": rule["EXTERNAL SERVER"],
                    "community": {
                      "name": communityName
                    }
                  }
                },
                "domain": {
                  "name": rule["EXTERNAL SERVER"]
                },
                "name": rule["EXTERNAL NAME"],
                "displayName": rule["EXTERNAL NAME"],
                "type": {
                  "name": "Column"
                },
                "attributes": attributes,
                "relations": {
                  "00000000-0000-0000-0000-000000007042:TARGET": [{
                    "name": rule["TABLE EXTERNAL NAME"],
                    "domain": {
                      "name": rule["EXTERNAL SERVER"],
                      "community": {
                        "name": communityName
                      }
                    }
                  }]
                }
              }
            ])
          })
        })
      });
    })  }

  postSendDataQualityProfiles(): PromiseLike<any> {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  }

  private authenticate(username: string, password: string): PromiseLike<any> {
    return new Promise((resolve: any, reject: any) => {
      log("Logging in...");
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
          log("Sign in successful.");
          this.cookie = response.headers["set-cookie"].join(" ");
          resolve(data);
        }
      }).catch((error) => {
        log("Session already found. Deleting...");
        log(error.code);
        log(error.response.data);
        axios.request({
          url: `${this.url}/rest/2.0/auth/sessions/current`,
          method: "DELETE",
          headers: { "Content-Type": "application/json" }
        }).then(() => {
          log("User logged out. Retrying...");
          resolve(this.authenticate(username, password));
        }).catch((error) => {
          log(error.response.data);
          reject(error);
        })
      });
    })
  }

  private importApi(args: any): PromiseLike<any> {
    let formData = new FormData();
    let fileData = Buffer.from(JSON.stringify(args));
    formData.append("file", fileData, "file.txt");
    formData.append("batchSize", 4000);
    formData.append("fileName", "file.txt");

    // @ts-ignore
    let formDataBuffer = formData.getBuffer();
    let headers = Object.assign(formData.getHeaders(), { "Cookie": this.cookie });

    return new Promise((resolve, reject) => {
      log(`Starting import now with arguments: ${JSON.stringify(args)}`);
      axios.request({ 
        url: `${this.url}/rest/2.0/import/json-job`,
        method: "POST",
        headers,
        data: formDataBuffer
      }).then((response) => {
        const jobId = response.data.id;
        log(`Import started. Job Id: ${jobId}`);

        let cont = setInterval(() => {
          log("Checking to see if import is complete...");
          return axios.request({
            method: "GET",
            url: `${this.url}/rest/2.0/jobs/${jobId}`,
            headers: { "Content-Type": "application/json", "Cookie": this.cookie },
          }).then(({ data }) => {
            if (data.state === "ERROR") {
              log(`Import failed.`);
              clearInterval(cont);
              reject(data);
            }
            else if (data.state === "COMPLETED") {
              log(`Import successfully completed.`);
              clearInterval(cont);
              resolve(data);
            }
          })
        }, 1000)
      }).catch((err) => {
        log(`Import not successful.`);
        log(err.response.data);
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
          log(`No relationType with name '${name}' found. Creating now...`);
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
            log(`RelationType with ID ${relationType.id} created successfully`);
            this["customRelations"][saveTo] = relationType.id;
            resolve(this.getOrCreateRelationTypes({coRole, role, sourceTypeId, targetTypeId, saveTo}))
          }).catch((error: any) => {
            log(`PostAssetError with parameters: ${JSON.stringify(args)}`);
            log(error.response.data);
            reject(error);
          })

        } else {
          this["customRelations"][saveTo] = data.results[0].id;
          log(`Id (${data.results[0].id}) for relation type saved. Continuing...`);
          resolve(this.getOrCreateRelationTypes({coRole, role, sourceTypeId, targetTypeId, saveTo}))
        }
      }).catch((error: any) => {
        log(`RelationTypeError with parameters: ${JSON.stringify(args)}`);
        log(error.response.data);
        reject(error);
      })
    }); 
  }
};

// Send rule data and profile data over.
const runner = new CollibraConnector();
 runner.retrieveAssets().then(() => {
   // runner.sendDataQualityRules();
  runner.sendDataQualityProfiles();
 });
