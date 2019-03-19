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

interface NormalizedAssetProperties {
  Instance: string; 
  Schema: string;
  Table: string;
  Column: string;
  DATA_TYPE: string;
  TYPE_NAME: string;
  COLUMN_SIZE: string;
  NULLABLE: string;
  COLUMN_DEF: string;
  SQL_DATA_TYPE: string;
  SQL_DATETIME_SUB: string;
  CHAR_OCTET_LENGTH: string;
  ORDINAL_POSITION: string;
  IS_NULLABLE: string;
}

interface ApiResult {
  pageSize: number; // e.g. 200;
  pageNum: number; // e.g. 1;
  total: number; // e.g. 486;
  items: TechnologyAsset[]; // e.g. see TechnologyAsset, below
}

interface TechnologyAsset { 
  AssetId: number; // e.g. 5300
  AssetUid: string; // e.g. '9572c64b-fc4f-4093-ae89-8022a738874d',
  AssetTypeId: number; // e.g. 86,
  AssetTypeUid: string; // e.g. 'e3fff74d-b1ce-4fee-b7f1-291781f88eee',
  UpdatedOn: string; // e.g. '2018-12-12T14:54:26.873Z',
  CreatedOn: string; // e.g. '2018-12-11T20:11:07.77Z',
  SourceID: string; // e.g. '[ADVENTUREWORKS].[SALES].[STORE].[SALESPERSONID]',
  Name: string; // e.g. '[Sales].[Store].[SalesPersonID]',
  AssetPath: string; // e.g. '[Sales].[Store].[SalesPersonID]',
  NormalizedAsset: string; // e.g. 'Database Table Column',
  BusinessAssetOwner: string; // e.g. 'AdventureWorks',
  Definition: string; // e.g. 'SQL Server Column',
  NormalizedAssetProperties: string; // e.g. '{Instance:"AdventureWorks",Schema:"Sales",Table:"Store",Column:"SalesPersonID",DATA_TYPE:"4",TYPE_NAME:"int",COLUMN_SIZE:"10",NULLABLE:"1",SQL_DATA_TYPE:"4",SQL_DATETIME_SUB:"null",CHAR_OCTET_LENGTH:"0",ORDINAL_POSITION:"3",IS_NULLABLE:"YES"}',
  TechnologyAsset: string; // e.g. 'SQL Server Column',
  TechnologyAssetProperties: string; // e.g. '{"BUFFER_LENGTH":"4","DECIMAL_DIGITS":"0","NUM_PREC_RADIX":"10","IS_AUTOINCREMENT":"NO"}',
  Glossary: boolean; // e.g. false
}

/* 
 * This interface says we will either have an object that looks like:
 * { SchemaName: { DatabaseName: { TableName: { ColumnName: UUID } } } }
 *
 * or 
 *
 * { DatabaseName: { TableName: { ColumnName: UUID } } }
 */
interface MetaDataMap {
  // Instance/Database
  [key: string]: {
    // Schema
    [key: string]: {
      // Table 
      [key: string]: { 
        // Column -> UUID
        [key: string]: string 
      };
    }
  }
}

export class Data3SixtyConnector extends Connector {

  // File object that contains config
  configuration: any;

  // Environment location of data3sixty, e.g. yourname.data3sixty.com
  url?: string;

  // API key associated with environment, e.g. 123
  apiKey?: string;
  
  // Secret associated with API key, e.g. xyz 
  apiSecret?: string;
  
  // Fusion attribute UID, e.g. 123xyZ 
  fusionAttributeUid?: string;

  // Date of the last run
  lastRun: Date;

  // Location to the HTTP ODBC API
  odbcUrl: string
  /*
   * A mapping of the asset metadata, e.g. Schema Table Column to the Technology Asset UUID.
   * So for example:
   * "SchemaName": {
   *  "DatabaseName": {
   *    "TableName": {
   *      "ColumnName": 'e3fff74d-b1ce-4fee-b7f1-291781f88eee'
   *    }
   *  }
   * }
   *
   * So with this, querying for all of the Database names would be as simple as doing:
   * Object.keys(SchemaName), or to get the UUID you would say
   * SchemaName.DatabaseName.TableName.ColumName
   */
  assetMetaDataToTechnologyAssetUuid: MetaDataMap;

  constructor() {
    super();

    this.configuration = jsonFile(`${path.resolve(__dirname, "..")}/connector-config.json`);

    const URL = this.configuration.get("Data3SixtyConnector.data3SixtyUrl");
    if (!URL) {
      throw "DATA3SIXTY_URL not found";
    } else {
      this.url = URL;
    }

    const API_KEY = this.configuration.get("Data3SixtyConnector.apiKey");
    if (!API_KEY) {
      throw "DATA3SIXTY_API_KEY not found"
    } else {
      this.apiKey = API_KEY;
    }

    const API_SECRET = this.configuration.get("Data3SixtyConnector.apiSecret");
    if (!API_SECRET) {
      throw "DATA3SIXTY_API_SECRET not found";
    } else {
      this.apiSecret = API_SECRET;
    }

    const FUSION_UID = this.configuration.get("Data3SixtyConnector.fusionAttributeUid");
    if (!FUSION_UID) {
      throw "DATA3SIXTY_FUSION_ATTRIBUTE_UID not found";
    } else {
      this.fusionAttributeUid = FUSION_UID;
    }

    const LAST_RUN = this.configuration.get("Data3SixtyConnector.lastRun");
    if (!LAST_RUN) {
      console.log("lastRun not found")
      console.log("Defaulting to 1900-01-01");
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

    this.assetMetaDataToTechnologyAssetUuid = {};

  };

  /*
   * Before getting assets from the source (typically 
   * a data governance center) do whatever clean-up you would like.
   * This might be checking authentication, for example.
   */
  preRetrieveAssets(): PromiseLike<any> {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  };

  /*
   * Get assets from a source. These assets might be Technology Assets
   * from Data3Sixty or getting a variety of assets for joining from 
   * another system.
   */
  retrieveAssets(): PromiseLike<any> {
    return this.preRetrieveAssets().then(() => {
      return new Promise((resolve: any, reject: any) => {
        axios.request({
          url: `${this.url}/api/v2/assets/${this.fusionAttributeUid}`,
          method: "GET",
          headers: {
            "Authorization": `${this.apiKey};${this.apiSecret}`
          },
        })
          .then((data) => {
            resolve(data);
            this.postRetrieveAssets(data.data);

          })
          .catch((error) => {
            reject(error);
          })
      });
    });
  };

  /*
   * After getting the assets do whatever clean-up you would like. 
   * This might be sending an email or another type of notification
   * to a system.
   */
  postRetrieveAssets(data: any): PromiseLike<any> {
    /*
     * For now we will just store everything in memory, but as the amount of
     * items we expect 'data' above to store increases we will need to eventually
     * move memory storage into something similar to redis, and query that, instead.
     *
     * If we go that route, we will also need to occassionally flush and rebuild 
     * the storage and/or remove unused keys for the purposes of efficency.
     */

    return new Promise((resolve: any, reject: any) => {
      tailored.defmatch(
        tailored.clause([{
          items: $
        }], (items: TechnologyAsset[]) => {
          items.forEach((item: TechnologyAsset) => {
            if (!this.assetMetaDataToTechnologyAssetUuid) {
              throw "Asset Metdata map not defined";
            }

            let normalizedProperties = this._normalizeJsonString(item.NormalizedAssetProperties)

            /*
             * Example final structure
             * (Instance) AdventureWorks: {
             *    (Schema) Sales: {
             *        (Table) SpecialOffer: {
             *            (Column) MinQty: (Uuid) 8c9e21f3-8613-4b00-a78b-8105e31c331f
             *        }
             *    }
             * }
             */

            // Build the tree structure (bottom up)

            const Column   = normalizedProperties.Column.toLocaleLowerCase();
            const Table    = normalizedProperties.Table.toLocaleLowerCase();
            const Schema   = normalizedProperties.Schema.toLocaleLowerCase();
            const Instance = normalizedProperties.Instance.toLocaleLowerCase();

            /*
             * (Column) MinQty: (Uuid) 8c9e21f3-8613-4b00-a78b-8105e31c331f
             */

            let ColumnUuid: { [key: string]: string }; 
            try {
              ColumnUuid = this.assetMetaDataToTechnologyAssetUuid[Instance][Schema][Table] || {};
            } catch(e) {
              ColumnUuid = {};
            }

            ColumnUuid[Column] = item.AssetUid;

            /*
             * (Table) SpecialOffer: *ColumnUuid*
             */
            let TableColumn: { [key: string]: { [key: string]: string } };
            try {
              TableColumn = this.assetMetaDataToTechnologyAssetUuid[Instance][Schema] || {};
            } catch(e) {
              TableColumn = {};
            }

            TableColumn[Table] = ColumnUuid;

            /*
             * (Schema) Sales: *TableColumn*
             */
            let SchemaTable: { [key: string]: { [key: string]: { [key: string]: string } } };
            try {
              SchemaTable = this.assetMetaDataToTechnologyAssetUuid[Instance] || {};
            } catch(e) {
              SchemaTable = {};
            }

            SchemaTable[Schema] = TableColumn;

            /*
             * (Instance) AdventureWorks: *SchemaTable*
             */ 
            this.assetMetaDataToTechnologyAssetUuid[Instance] =
              SchemaTable;

          })
          
          resolve();
        }),
        tailored.clause([_], () => {
          // We want to throw an error if there are no actual
          // TechnologyAssets to query against. 
          reject("Items were not found in response");
        })
      )(data);
    })
  };

  /*
   * Before sending the Data Quality Rules do whatever clean-up you
   * would like. This might be checking to see if data quality rules exist
   * before retrieval.
   */
  preSendDataQualityRules(): PromiseLike<any> {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  };

  /*
   * Send data quality rules to the system. The rules that are chosen to 
   * be sent will depend on the configuration options you have selected. 
   */
  sendDataQualityRules(): PromiseLike<any>  {
    return this.preSendDataQualityRules().then(() => {
      return new Promise((resolve: any, reject: any) => {
        let month = String(this.lastRun.getMonth() + 1);
        if (month.length === 1) {
          month = "0" + month;
        }

        let day = String(this.lastRun.getDate());
        if (day.length === 1) {
          day = "0" + day;
        }

        let year = String(this.lastRun.getFullYear());
        if (year.length === 1) {
          year = "0" + year;
        }

        let hours = String(this.lastRun.getHours());
        if (hours.length === 1) {
          hours = "0" + hours;
        }

        let minutes = String(this.lastRun.getMinutes());
        if (minutes.length === 1){
          minutes = "0" + minutes;
        }

        let milliseconds = String(this.lastRun.getMilliseconds());
        if (milliseconds.length === 1) {
          milliseconds = "0" + milliseconds;
        }

        const safeDateTimeString = `${year}-${month}-${day} ${hours}:${minutes}:${milliseconds}`;

        axios.request({
          url: `http://${this.odbcUrl}/query`,
          method: "POST",
          headers: { "Content-Type": "application/json" },
          data: {
            sql: `SELECT * FROM "RULES" WHERE "VERSIONS OFFSET" = 0 AND "LAST VALIDATED" > '${safeDateTimeString}'`
          }
        }).then((odbcData) => {


          if (odbcData.data.length === 0) {
            console.log("No data found (either no rules have been created, or no new rules have been validated)");
          }

          let postPromises: PromiseLike<any>[] = [];

          for (let i = 0; i < odbcData.data.length; i++) {
            const rule: PandoraRule = odbcData.data[i];

            const database = rule["EXTERNAL DATABASE"].toLocaleLowerCase();
            const schema = rule["EXTERNAL SCHEMA"].toLocaleLowerCase();
            const table = rule["EXTERNAL TABLE NAME"].toLocaleLowerCase();
            const column = rule["EXTERNAL COLUMN NAME"].toLocaleLowerCase();

            let uid: string;
            try  {
              uid = this.assetMetaDataToTechnologyAssetUuid[database][schema][table][column];
            } catch(e) {
              console.log(`Mapping not found to: ${database}-${schema}-${table}-${column}, continuing...`);
              continue;
            }

            const ruleUid = rule.DESCRIPTION.match(/.*ruleUid=([^;]+)/)

            if (ruleUid) {
              const postPromise = new Promise((resolve, reject) => {
                axios.request({
                  url: `${this.url}/api/v2/dataquality/${ruleUid[1]}`,
                  method: "POST",
                  headers: {
                    "Authorization": `${this.apiKey};${this.apiSecret}`
                  },
                  data: {
                    "Results": [
                      {
                        "Result": {
                          "PassCount": rule["ROWS PASSED"],
                          "FailCount": rule["ROWS FAILED"],
                          "EffectiveDate": new Date(rule["LAST VALIDATED"]),
                          "RunDate": new Date().toJSON()
                        },
                        "AssetsMappings": [
                          {
                            "AssetPath": "string",
                            "AssetUID": uid
                          }
                        ]
                      }
                    ]
                  }
                }).then((data) => {
                  resolve(data);
                  console.log(`Rule: '${rule["NAME"]}' sent successfully.`)
                }).catch((err) => {
                  reject(err);
                  console.log(err);
                })
              });

              postPromises.push(postPromise);
            }
          }

          Promise.all(postPromises).then((data) => {
            if (data.length > 0) {
              this.configuration.set("Data3SixtyConnector.lastRun", new Date().toString());
              this.configuration.save();
            }

            this.postSendDataQualityRules().then(() => {
            });
          });
        })
      });
    });
  };

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

  private _normalizeJsonString(jsonString: string): NormalizedAssetProperties {
    return tailored.defmatch(
      tailored.clause([$], (value: string) => {
        // Converts { Key: "Value" } to { "Key": "Value" }
        return JSON.parse(value.replace(/(\w+):/g, function(match, p1) { return `"${p1}":` }))
      }),
      tailored.clause([_], () => {
        throw "No string was input."
      })
    )(jsonString);
  }
};

const runner = new Data3SixtyConnector();
runner.retrieveAssets().then(() => {
  runner.sendDataQualityRules()
})
