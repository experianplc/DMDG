import Connector from "./connector";
import * as tailored from "tailored";
//@ts-ignore
import jsonFile from "edit-json-file";
import axios from "axios";
import * as EventEmitter from "events";
import produce from "immer";
import Promise from "bluebird";
import path from "path";

const $ = tailored.variable();
const _ = tailored.wildcard();
const debug = true;

function log(message: string) {
  if (debug) {
    console.log(message);
  }
}

interface RelationMap {
  "EXTERNAL DATABASE": string;
  "EXTERNAL COLUMN NAME": string;
  "EXTERNAL TABLE NAME": string;
  "RULE": string;
  "DIMENSION"?: string;
  "DQ METRIC": string;
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


interface RelationAttributeArguments {
  sourceId: string;
  targetId: string;
  typeId: string;
  startingDate?: number;
  endingDate?: number;
  noDeletion?: boolean;
};

interface RelationGetResponse {
  total: number;
  offset: number;
  limit: number;
  results: RelationResult[];
};

interface RelationResult {
  source: {
    name: string,
    id: string,
    resourceType: string
  },
  target: {
    name: string,
    id: string,
    resourceType: string
  },
  type: {
    id: string
    resourceType: string
  },
  startingDate: number,
  endingDate: number,
  createdBy: string,
  createdOn: number,
  lastModifiedBy: string,
  lastModifiedOn: number,
  system: boolean,
  resourceType: string,
  id: string
}


interface AssetResult {
  displayName: string;
  articulationScore: number;
  excludedFromAutoHyperlinking: boolean;
  domain: {
    name: string;
    id: string;
    resourceType: string;
  },
  type: {
    name: string;
    id: string;
    resourceType: string;
  },
  status: {
    name: string;
    id: string;
    resourceType: string;
  },
  avgRating: number;
  ratingsCount: number;
  name: string;
  createdBy: string;
  createdOn: number;
  lastModifiedBy: string;
  lastModifiedOn: number;
  system: boolean;
  resourceType: string;
  id: string;
}

interface AssetGetResponse {
  total: number;
  offset: number
  limit: number;
  results: AssetResult[]; 
}

interface AttributeGetResponse {
  total: number,
  offset: number,
  limit: number;
  results: AttributeResult[]
}

interface AttributeResult {
  type: {
    name: string,
    id: string,
    resourceType: string
  },
  asset: {
    name: string,
    id: string,
    resourceType: string
  },
  createdBy: string,
  createdOn: number,
  lastModifiedBy: string,
  lastModifiedOn: number,
  system: boolean,
  resourceType: string;
  id: string;
}

interface CommunityResult {
  id: string;
  createdBy: string;
  createdOn: number;
  lastModifiedBy: string;
  lastModifiedOn: number;
  system: boolean;
  resourceType: string;
  name: string;
  description: string;
}

interface CommunityGetResponse {
  total: number;
  offset: number;
  limit: number;
  results: CommunityResult[]
}

interface DomainGetResponse {
  total: number;
  offset: number;
  limit: number;
  results: DomainResult[];
}

interface DomainResult {
  type: {
    name: string,
      id: string,
      resourceType: string
  };
  community: {
    name: string,
      id: string,
      resourceType: string
  };
  excludedFromAutoHyperlinking: boolean;
  description: string;
  name: string;
  createdBy: string;
  createdOn: number;
  lastModifiedBy: string;
  lastModifiedOn: number;
  system: boolean;
  resourceType: string;
  id: string;
}

interface AssetAttributeArguments {
  assetId: string;
  typeId: string | string[];
  value: any;
}

interface AssetArguments {
  name: string; 
  displayName?: string;
  domainId: string; 
  typeId: string;
}

interface DomainArguments {
  name: string;
  communityId: string;
  typeId: string;
  description?: string;
}

  // Collibra Mapping
  // Ideally this entire thing is portable so it can be transfered to another
  // system in a relatively straight-forward fashion.
  /*
   * Should look something like this:
   *
   * {
   *  communityId: ID GOES HERE
   *  - domain id -: {
   *    domainId: ID GOES HERE
   *    - asset id -: {
   *      assetId: ID GOES HERE
   *      - attribute id -: {
   *      },
   *    }
   * }
   */

interface Other {
  [key: string]: string;
}

/*
  interface MetaDataMap {
    [key: string]: string | { // Community ids
      [key: string]: string | { // Domain ids
        [key: string]: string | { // Asset ids
          [key: string]: string     // Attribute Ids
        }
      }
    }
  }
*/

export class CollibraConnector extends Connector {

  // File object that contains config
  configuration: any;

  // Environment location of the collibra environment, e.g. https://experian-dev-54.collibra.com/
  url?: string;

  // Date of the last run
  lastRun: Date;

  // Location to the HTTP ODBC API
  odbcUrl: string

  metadataMap: any;

  // Used to make subsequent API requests to the Collibra Data Governance Center API.
  sessionToken: string;
  cookie: string;

  constructor() {
    super();

    this.configuration = jsonFile(`${path.resolve(__dirname, "..")}/connector-config.json`);

    const URL = this.configuration.get("CollibraConnector.collibraUrl");
    if (!URL) {
      throw "COLLIBRA_URL not found";
    } else {
      this.url = URL;
    }

    const LAST_RUN = this.configuration.get("CollibraConnector.lastRun");
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

    this.metadataMap = {};
    this.sessionToken = "";
    this.cookie = "";
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
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
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
    // TODO: Move to config
    const username = "Admin";
    const password = "Password123";
    return this.authenticate(username, password);
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
          typeId: "00000000-0000-0000-0000-000000030003", 
          description: "Data quality imported from Experian" 
        },
        { 
          name: "Data Quality Rules", 
          typeId: "00000000-0000-0000-0000-000000030023", 
          description: "Data quality rules" 
        }
      ];

      let communityId: string; 

      // Create community if non existent
      this.getOrCreateCommunity(communityName, communityDescription).then((id) => {
        communityId = id;

        let promises: PromiseLike<string>[] = [];
        domains.forEach((domain) => {
          promises.push(this.getOrCreateDomain({
            name: domain.name,
            communityId,
            typeId: domain.typeId,
            description: domain.description
          }))
        });

        return Promise.all(promises);
      }).then((domainIds: any) => {
        domainIds.forEach((domainId: string, index: number) => {
          this.metadataMap[communityId][domains[index]["name"]] = domainId;
        })
        
        axios.request({ 
          url: `${this.odbcUrl}/query`,
          method: "POST",
          headers: { "Content-Type": "application/json" },
          data: {
            sql: `SELECT * FROM \"RULES\" WHERE \"VERSIONS OFFSET\" = 0`,
          }
        }).then((response: any) => {
          const rules: PandoraRule[] = response.data;
          const dataAssetDomainTypeId = "00000000-0000-0000-0000-000000030001";
          const dataAssetDomainDescription = "Assets from database";

          const rulebookId = this.metadataMap[communityId]["Data Quality Rules"];
          const rulebookTypeId = "00000000-0000-0000-0000-000000031205";

          // Asset types (move to environment variable)
          const dataQualityMetricTypeId = "00000000-0000-0000-0000-000000031107";
          const dataQualityRuleTypeId = "00000000-0000-0000-0000-000000031205"
          const databaseTypeId = "00000000-0000-0000-0000-000000031006"
          const tableTypeId = "00000000-0000-0000-0000-000000031007"
          const columnTypeId = "00000000-0000-0000-0000-000000031008"
          let metaPromises: any = [];

          // It is possible for each rule to hypothetically refer to assets that are in separate
          // data asset domains (denoted by being in separate databases), so it is necessary to do
          // the check for each rule.
          rules.forEach((rule: PandoraRule) => {
            this.getOrCreateDomain({
              name: rule["EXTERNAL SERVER"],
              communityId,
              typeId: dataAssetDomainTypeId,
              description: dataAssetDomainDescription
            }).then((dataAssetDomainId) => {

              /*
               * This isn't the best practice, but for now we're
               * going to use the indicies in order to store location information
               *
               */
              let promises: PromiseLike<string>[] = [];

              // This contains information on the promises.

              promises.push(
                this.getOrCreateAsset({
                  name: rule["EXTERNAL DATABASE"],
                  domainId: dataAssetDomainId,
                  typeId: databaseTypeId
                }).then((id) => {
                  metaPromises.push(["EXTERNAL DATABASE", id]);
                  return id;
                })
              ); // Create relationship to Server


              promises.push(
                this.getOrCreateAsset({
                name: rule["EXTERNAL TABLE NAME"],
                domainId: dataAssetDomainId,
                typeId: tableTypeId
                }).then((id) => {
                  metaPromises.push(["EXTERNAL TABLE NAME", id]);
                  return id;
                })
              ); // Create relationship to Database

              promises.push(
                this.getOrCreateAsset({
                name: rule["EXTERNAL COLUMN NAME"],
                domainId: dataAssetDomainId,
                typeId: columnTypeId
                }).then((id) => {
                  metaPromises.push(["EXTERNAL COLUMN NAME", id]);
                  return id;
                })
              ); // Create relationship to Table

              const ruleMatch = rule["DESCRIPTION"].match(/rule=([^;]*)/); 
              if(ruleMatch && ruleMatch.length === 2) {
                promises.push(
                  this.getOrCreateAsset({
                  name: ruleMatch[1],
                  domainId: rulebookId,
                  typeId: rulebookTypeId
                  }).then((id) => {
                    metaPromises.push(["RULE", id]);
                    return id;
                  })
                ) // Create relationship to Results
              }

              //  Create Data Quality Metrics and add them to Governance Asset Domain if nonexistent
              promises.push(this.getOrCreateAsset({
                name: rule["NAME"],
                domainId: this.metadataMap[communityId]["Data Quality Results"],
                typeId: dataQualityMetricTypeId
              }).then((assetId: string) => {

                metaPromises.push(["DQ METRIC", assetId]);

                // Create attributes for Data Quality Metric
                const thresholdMatch = rule["PASS RANGE"].match(/(\d+).*/);
                if (thresholdMatch) {
                  const thresholdTypeId = "00000000-0000-0000-0000-000000000239";
                  this.createAttribute({
                    assetId,
                    typeId: thresholdTypeId,
                    value: Number(thresholdMatch[1])
                  })
                }

                const descriptionMatch = rule["DESCRIPTION"].match(/description=([^;]*)/)
                if (descriptionMatch) {
                  const descriptionTypeId = "00000000-0000-0000-0000-000000003114";
                  this.createAttribute({
                    assetId,
                    typeId: descriptionTypeId,
                    value: descriptionMatch[1]
                  });
                }

                const exampleMatch = rule["DESCRIPTION"].match(/example=([^;]*)/)
                if (exampleMatch) {
                  const descriptiveExampleMatchTypeId = "00000000-0000-0000-0000-000000003115";
                  this.createAttribute({
                    assetId,
                    typeId: descriptiveExampleMatchTypeId,
                    value: exampleMatch[1]
                  });
                }

                const loadedRowsTypeId = "00000000-0000-0000-0000-000000000233";
                this.createAttribute({
                  assetId,
                  typeId: loadedRowsTypeId,
                  value: Number(rule["ROWS CONSIDERED"])
                })

                const rowsPassedTypeId = "00000000-0000-0000-0000-000000000236";
                const conformityScoreTypeId = "00000000-0000-0000-0001-000500000021";
                this.createAttribute({
                  assetId,
                  typeId: rowsPassedTypeId,
                  value: Number(rule["ROWS PASSED"])
                })

                this.createAttribute({
                  assetId,
                  typeId: conformityScoreTypeId,
                  value: Number(rule["ROWS PASSED"])
                })

                const rowsFailedTypeId = "00000000-0000-0000-0000-000000000237";
                const nonConformityScoreTypeId = "00000000-0000-0000-0001-000500000022";
                this.createAttribute({
                  assetId,
                  typeId: rowsFailedTypeId,
                  value: Number(rule["ROWS FAILED"])
                })

                this.createAttribute({
                  assetId,
                  typeId: nonConformityScoreTypeId,
                  value: Number(rule["ROWS FAILED"])
                })

                const resultTypeId = "00000000-0000-0000-0000-000000000238";
                this.createAttribute({
                  assetId,
                  typeId: resultTypeId,
                  value: rule["RESULT"] === "Green" ? true : false
                })

                const passingFractionTypeId = "00000000-0000-0000-0000-000000000240"; 
                this.createAttribute({
                  assetId,
                  typeId: passingFractionTypeId,
                  value: (Number(rule["ROWS PASSED"]) / Number(rule["ROWS CONSIDERED"])) * 100
                })

                const lastSyncDateTypeId = "00000000-0000-0000-0000-000000000256";
                if (rule["LAST VALIDATED"] && rule["LAST VALIDATED"] !== "None") { 
                  this.createAttribute({
                    assetId,
                    typeId: lastSyncDateTypeId,
                    value: new Date((rule["LAST VALIDATED"] + " UTC").replace(/\d\d:\d\d:\d\d/, "00:00:00")).getTime()
                  })
                }

                return assetId;
              }));

              promises.push(metaPromises); 
              return Promise.all(promises);
            }).then((ids: any) => {
              debugger;
              let promises: PromiseLike<string>[] = [];
              const relationMap: RelationMap = this._fromEntries(ids[ids.length - 1]);
              const tableDatabaseRelationTypeId = "00000000-0000-0000-0000-000000007045"; 
              promises.push(
                this.createRelation({
                  sourceId: relationMap["EXTERNAL TABLE NAME"],
                  targetId: relationMap["EXTERNAL DATABASE"],
                  typeId: tableDatabaseRelationTypeId,
                  noDeletion: true
                })
              );

              const columnTableRelationTypeId = "00000000-0000-0000-0000-000000007042"; 
              promises.push( 
                this.createRelation({
                  sourceId: relationMap["EXTERNAL COLUMN NAME"],
                  targetId: relationMap["EXTERNAL TABLE NAME"],
                  typeId: columnTableRelationTypeId,
                  noDeletion: true
                })
              );

              const columnMetricTypeId = "00000000-0000-0000-0000-000000007018";
              promises.push( 
                this.createRelation({
                  sourceId: relationMap["EXTERNAL COLUMN NAME"],
                  targetId: relationMap["DQ METRIC"],
                  typeId: columnMetricTypeId,
                  noDeletion: true
                })                      
              )
              const ruleMetricTypeId = "00000000-0000-0000-0000-000000007016"; 
              promises.push( 
                this.createRelation({
                  sourceId: relationMap["RULE"],
                  targetId: relationMap["DQ METRIC"],
                  typeId: ruleMetricTypeId,
                  noDeletion: true
                })                      
              );

              if (relationMap["DIMENSION"]) {
                const dimensionMetricTypeId = "00000000-0000-0000-0000-000000007053";
                promises.push( 
                  this.createRelation({
                    sourceId: relationMap["DIMENSION"],
                    targetId: relationMap["DQ METRIC"],
                    typeId: dimensionMetricTypeId,
                    noDeletion: true
                  })                      
                );
              }

              return Promise.all(promises);
            });
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
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  }

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
          this.sessionToken = data["csrfToken"];
          this.cookie = response.headers["set-cookie"].join(" ");
          resolve(data);
        }
      }).catch((error) => {
        log(error.response.data);
        log("Session already found. Deleting...");
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

  private getOrCreateCommunity(name: string, description: string): PromiseLike<any> {
    const communityId = Object.keys(this.metadataMap)[0];
    if (communityId) {
      log(`Community with id: ${communityId} found.`);
      return new Promise((resolve: any) => {
        resolve(communityId);
      })
    }

    return new Promise((resolve: any, reject: any) => {
      log("Community ID not saved. Checking to see if it exists in the Governance Center...");
      const self = this;
      axios.request({ 
        url: `${this.url}/rest/2.0/communities`,
        method: "GET",
        headers: { "Cookie": this.cookie },
        params: {
          name: name,
        }
      }).then((response) => {
        const data: CommunityGetResponse = response.data;

        if (data.total === 0) {
          log(`No community with name '${name}' found.`);
          log("Creating community...");
          axios.request({ 
            url: `${this.url}/rest/2.0/communities`,
            method: "POST",
            headers: { "Content-Type": "application/json", "Cookie": this.cookie },
            data: {
              name,
              description,
            }
          }).then((response) => {
            const communityResult: CommunityResult = response.data;
            log(`Community with name ${communityResult.name} created successfully.`);
            this.metadataMap[communityResult.id] = {};
            resolve(this.getOrCreateCommunity(name, description));
          }).catch((err) => reject(err));
        } else {
          log("Community ID found in governance center. Saving locally...");
          const result: CommunityResult  = data.results[0];
          this.metadataMap[result.id] = {};
          resolve(this.getOrCreateCommunity(name, description));
        }
      }).catch((err) => { reject(err) });    
    });
  }

  private getOrCreateDomain(args: DomainArguments): PromiseLike<any> {
    let { name, communityId, typeId, description } = args;

    if (!communityId) { 
      throw "DomainCreationError: Something went wrong. A community is not specified.";
    }

    if (!typeId) {
      throw "DomainCreationError: Something went wrong. A typeId is not specified.";
    }

    if (this.metadataMap[communityId][name]) {
      log(`Id (${this.metadataMap[communityId][name]} for domain '${name}' saved. Continuing...`);
      return new Promise((resolve: any, reject: any) => {
        resolve(this.metadataMap[communityId][name]);
      });
    }

    /* In effect, this checks to see if the Domain exists. 
     * If it does exist, we take its value and save it, continuing.
     * If it does not exist we create it and save its value, continuing.
     */
    return new Promise((resolve: any, reject: any) => {
      axios.request({ 
        url: `${this.url}/rest/2.0/domains`,
        method: "GET",
        headers: { "Content-Type": "application/json", "Cookie": this.cookie },
        params: {
          name: name,
          communityId: Object.keys(this.metadataMap)[0],
        }
      }).then((response) => {
        const data: DomainGetResponse = response.data;

        if (data.total === 0) {
          log(`No domain with name '${name}' found. Creating now...`);
          axios.request({ 
            url: `${this.url}/rest/2.0/domains`,
            method: "POST",
            headers: { "Content-Type": "application/json", "Cookie": this.cookie },
            data: {
              name,
              communityId: Object.keys(this.metadataMap)[0],
              typeId, 
              description,
            }
          }).then((response: any) => {
            const domainResult: DomainResult = response.data;
            log(`Domain with name ${domainResult.name} created successfully.`);
            this.metadataMap[communityId][name] = domainResult.id;
            this.metadataMap[communityId][domainResult.id] = {};
            resolve(this.getOrCreateDomain({name, communityId, typeId, description}))
          }).catch((error: any) => {
            reject(error);
          })

        } else {
          const domainResult: DomainResult = data.results[0];
          this.metadataMap[communityId][name] = domainResult.id;
          this.metadataMap[communityId][domainResult.id] = {};
          resolve(this.getOrCreateDomain({name, communityId, typeId, description}))
        }
      }).catch((error: any) => {
        reject(error);
      })
    }) 
  };

  private createRelation(args: RelationAttributeArguments): PromiseLike<any> {
    const { sourceId, targetId, typeId, noDeletion } = args;
    let { startingDate, endingDate } = args

    if (!startingDate) {
      startingDate = 1488016800;
    }

    if (!endingDate) {
      endingDate = 1658021800;
    }

    if (!sourceId) { 
      throw "RelationCreationError: Something went wrong. A sourceId is not specified.";
    }

    if (!targetId) { 
      throw "RelationCreationError: Something went wrong. A targetId is not specified.";
    }

    if (!typeId) {
      throw "RelationCreationError: Something went wrong. A typeId is not specified.";
    }


    return new Promise((resolve: any, reject: any) => {
      axios.request({ 
        url: `${this.url}/rest/2.0/relations`,
        method: "GET",
        headers: { "Content-Type": "application/json", "Cookie": this.cookie },
        params: {
          sourceId,
          targetId,
          relationshipTypeId: typeId
        }
      }).then((response) => {
        const data: RelationGetResponse = response.data;

        if (data.total === 0) {
          log(`No relation with type '${typeId}', sourceId 
            '${sourceId} and targetId '${targetId}' found`
          );
          log(`Creating relation between ${sourceId} and ${targetId} now`);
          axios.request({ 
            url: `${this.url}/rest/2.0/relations`,
            method: "POST",
            headers: { "Content-Type": "application/json", "Cookie": this.cookie },
            data: {
              sourceId,
              targetId,
              typeId
            }
          }).then((response: any) => {
            const relationResult: RelationResult = response.data;
            log(`Relation between ${sourceId} and ${targetId} successfully created.`);
            return new Promise((resolve) => {
              resolve(relationResult.id);
            })
          }).catch((error: any) => {
            log(`PostRelationError with parameters: ${JSON.stringify(args)}`);
            log(error.response.data);
            reject(error);
          })

        } else {
          const relationToBeDeletedId = data.results[0].id;

          if (noDeletion) {
            return new Promise((resolve) => {
              return relationToBeDeletedId;
            })
          }

          axios.request({
            url: `${this.url}/rest/2.0/attributes/${relationToBeDeletedId}`,
            method: "DELETE",
            headers: { "Content-Type": "application/json", "Cookie": this.cookie },
          }).then((data: any) => {
            log(`Relation with ID: ${relationToBeDeletedId} deleted`);
            return this.createRelation(args);
          }).catch((error: any) => {
            log(`DeleteRelationError with parameters: ${JSON.stringify(args)}`);
            log(error.response.data);
            reject(error);
          })
        }
      }).catch((error: any) => {
        log(`GetRelationError with parameters: ${JSON.stringify(args)}`);
        log(error.response.data);
      })
    }); 
  }
  private createAttribute(args: AssetAttributeArguments): PromiseLike<any> {
    const { assetId, typeId, value } = args;

    if (!assetId) { 
      throw "AttributeCreationError: Something went wrong. An asset is not specified.";
    }

    if (!typeId) {
      throw "AttributeCreationError: Something went wrong. A typeId is not specified.";
    }

    return new Promise((resolve: any, reject: any) => {
      axios.request({ 
        url: `${this.url}/rest/2.0/attributes`,
        method: "GET",
        headers: { "Content-Type": "application/json", "Cookie": this.cookie },
        params: {
          assetId,
          typeIds: typeId
        }
      }).then((response) => {
        const data: AttributeGetResponse = response.data;

        if (data.total === 0) {
          log(`No attribute with type '${typeId}' found in asset '${assetId}'.`);
          log(`Creating attribute with value: ${value} now...`);
          axios.request({ 
            url: `${this.url}/rest/2.0/attributes`,
            method: "POST",
            headers: { "Content-Type": "application/json", "Cookie": this.cookie },
            data: {
              assetId,
              typeId,
              value
            }
          }).then((response: any) => {
            const attributeResult: AttributeResult = response.data;
            log(`Attribute with type ${typeId} created successfully in asset '${assetId}'.`);
            return new Promise((resolve) => {
              resolve(attributeResult.id);
            })
          }).catch((error: any) => {
            log(`PostAttributeError with parameters: ${JSON.stringify(args)}`);
            log(error.response.data);
            reject(error);
          })

        } else {
          const attributeToBeDeletedId = data.results[0].id;
          axios.request({
            url: `${this.url}/rest/2.0/attributes/${attributeToBeDeletedId}`,
            method: "DELETE",
            headers: { "Content-Type": "application/json", "Cookie": this.cookie },
          }).then((data: any) => {
            log(`Attribute with ID: ${attributeToBeDeletedId} deleted`);
            return this.createAttribute({assetId, value, typeId});
          }).catch((error: any) => {
            log(`DeleteAttributeError with parameters: ${JSON.stringify(args)}`);
            log(error.response.data);
            reject(error);
          })
        }
      }).catch((error: any) => {
        log(`GetAttributeError with parameters: ${JSON.stringify(args)}`);
        log(error.response.data);
      })
    }); 
  }

  private getOrCreateAsset(args: AssetArguments): PromiseLike<any> {
    const { name, displayName, domainId, typeId } = args;

    if (!domainId) { 
      throw "AssetCreationError: Something went wrong. A domain is not specified.";
    }

    if (!typeId) {
      throw "AssetCreationError: Something went wrong. A typeId is not specified.";
    }

    const communityId = Object.keys(this.metadataMap)[0];
    if (this.metadataMap[communityId][domainId][name]) {
      return new Promise((resolve: any) => {
        resolve(this.metadataMap[communityId][domainId][name])
      })
    }

    /* In effect, this checks to see if the Asset exists in the domain
     * If it does exist, we take its value and patch it, continuing.
     * If it does not exist we create it and save its value, continuing.
     */
    return new Promise((resolve: any, reject: any) => {
      axios.request({ 
        url: `${this.url}/rest/2.0/assets`,
        method: "GET",
        headers: { "Content-Type": "application/json", "Cookie": this.cookie },
        params: {
          name,
          domainId,
          typeId,
        }
      }).then((response) => {
        const data: AssetGetResponse = response.data;

        if (data.total === 0) {
          log(`No asset with name '${name}' found in domain '${domainId}'. Creating now...`);
          axios.request({ 
            url: `${this.url}/rest/2.0/assets`,
            method: "POST",
            headers: { "Content-Type": "application/json", "Cookie": this.cookie },
            data: {
              name,
              domainId,
              typeId, 
              displayName,
            }
          }).then((response: any) => {
            const assetResult: AssetResult = response.data;
            log(`Asset with name ${assetResult.name} created successfully in domain '${domainId}'.`);
            this.metadataMap[communityId][domainId][assetResult.id] = {};
            this.metadataMap[communityId][domainId][name] = assetResult.id;
            resolve(this.getOrCreateAsset({name, displayName, domainId, typeId}))
          }).catch((error: any) => {
            log(`PostAssetError with parameters: ${JSON.stringify(args)}`);
            log(error.response.data);
            reject(error);
          })

        } else {
          const assetResult: AssetResult = data.results[0];
          this.metadataMap[communityId][domainId][assetResult.id] = {}; // Add object for attributes
          this.metadataMap[communityId][domainId][name] = assetResult.id;
          log(`Id (${assetResult.id} for asset '${name}' saved. Continuing...`)
          resolve(this.getOrCreateAsset({name, displayName, domainId, typeId}))
        }
      }).catch((error: any) => {
        log(`GetAssetError with parameters: ${JSON.stringify(args)}`);
        log(error.response.data);
        reject(error);
      })
    }); 
  }

  /*
   * Transforms [[key1, val1], [key2, val2]] into
   * {
   *    key1: val1,
   *    key2: val2
   * }
   */
  private _fromEntries(array: any[]): any {
    let map: any = {};
    array.forEach((item: [any, any]) => {
      let [ key, value ] = item;
      map[key] = value;
    })

    return map;
  }
};

// Send rule data and profile data over.
const runner = new CollibraConnector();
 runner.retrieveAssets().then(() => {
  runner.sendDataQualityRules();
  runner.sendDataQualityProfiles();
});
