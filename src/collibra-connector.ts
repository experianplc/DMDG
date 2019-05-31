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


export class CollibraConnoctor extends Connector {

  // File object that contains config
  configuration: any;

  // Environment location of the collibra environment, e.g. https://experian-dev-54.collibra.com/
  url?: string;

  // Date of the last run
  lastRun: Date;

  // Location to the HTTP ODBC API
  odbcUrl: string

  constructor() {
    super();

    this.configuration = jsonFile(`${path.resolve(__dirname, "..")}/connector-config.json`);

    const URL = this.configuration.get("Data3SixtyConnector.data3SixtyUrl");
    if (!URL) {
      throw "COLLIBRA_URL not found";
    } else {
      this.url = URL;
    }

    const LAST_RUN = this.configuration.get("CollibraConnector.lastRun");
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
      // Check to see if there are any items being returned from HTTP-ODBC
      axios.request({ 
        url: `http://${this.odbcUrl}/query`,
        method: "POST",
        headers: { "Content-Type": "application/json" },
        data: {
          sql: `SELECT * FROM "RULES"`,
        }
      }).then((profileData) => {

      // Create community if non existent
      /*
        For each run:
        - Create community if nonexistent
        - Create "Governance Asset Domain" if nonexistent
        - Create "Rulebook" if nonexistent
        - Create "Data Asset Domain" with Database as name if nonexistent
       
        For each rule:
          * Create Database asset and add to Data Asset Domain if nonexistent
          * Create Table asset and add to Data Asset Domain if nonexistent
          * Create column assets and add to Data Asset Domain if nonexistent
          * Create rules and add to Rulebook if nonexistent
          * Create Data Quality Metrics and add them to Governance Asset Domain if nonexistent
          
          * Create relations from Data Quality Metric to Asset, Data Quality Rule and Data Quality
            Dimension (optionally)
          * Create relation from DQM to DQR
          * Create relation from DQM to DQD
     */
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


  preSendDataQualityProfiles(): PromiseLike<any> {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  }

  sendDataQualityProfiles(): PromiseLike<any> {
    return this.preSendDataQualityProfiles().then(() => {
      return new Promise((resolve: any, reject: any) => {
        axios.request({ 
          url: `http://${this.odbcUrl}/query`,
          method: "POST",
          headers: { "Content-Type": "application/json" },
          data: {
            sql: `SELECT * FROM "COLUMNS"`,
          }
        }).then((profileData) => {
          if (profileData.data.length === 0) {
            console.log("No data found (either no rules have been created, or no new rules have been validated)");
          }

          let postPromises: PromiseLike<any>[] = [];

          for (let i = 0; i < profileData.data.length; i++) {
            const rule: PandoraProfile = profileData.data[i];

            const database = rule["EXTERNAL DATABASE"].toLocaleLowerCase();
            const schema = rule["SCHEMA EXTERNAL NAME"].toLocaleLowerCase();
            const table = rule["TABLE EXTERNAL NAME"].toLocaleLowerCase();
            const column = rule["EXTERNAL NAME"].toLocaleLowerCase();

            let uid: string;
            try  {
              uid = this.assetMetaDataToTechnologyAssetUuid[database][schema][table][column];
            } catch(e) {
              console.log(`Mapping not found to: ${database}-${schema}-${table}-${column}, continuing...`);
              continue;
            }

            console.log(`Rule found: ${rule}`);
            const postPromise = new Promise((resolve, reject) => {
              axios.request({
                url: `${this.url}/api/v2/profiles/`,
                method: "POST",
                headers: {
                  "Authorization": `${this.apiKey};${this.apiSecret}`
                },
                data: [
                  {
                    "AssetUid": uid,
                    "RowCount": rule["ROW COUNT"],
                    "Uniqueness": rule["UNIQUENESS"],
                    "UniqueCount": rule["UNIQUE COUNT"],
                    "Completeness": rule["COMPLETENESS"],
                    "NullCount": rule["NULL COUNT"],
                    "BlankCount": rule["NULL COUNT"],
                    "DataType": rule["DOMINANT DATATYPE"],
                    "MinimumValue": rule["MINIMUM"],
                    "MaximumValue": rule["MAXIMUM"],
                    "Precision": rule["PRECISION"],
                    "Scale": rule["SCALE"],
                    "Average": rule["AVERAGE"],
                    "Median": 0, /* Pandora doesn't support this */
                    "StandardDeviation": rule["STANDARD DEVIATION OF VALUES"],
                    "Top10Values": [
                      {
                        "Value": rule["MOST COMMON VALUE"],
                        "Count": rule["COUNT OF MOST COMMON VALUES"]
                      }
                    ],
                    "ProcessIdentifier": rule["ID"]
                  }
                ]
              }).then((data) => {
                resolve(data);
                console.log(`PROFILE DATA FOR: '${rule["NAME"]}' sent successfully.`)
              }).catch((err) => {
                reject(err);
                console.log(err);
              })
            });

            postPromises.push(postPromise);
          }

          Promise.all(postPromises).then((data) => {
            if (data.length > 0) {
              this.configuration.set("Data3SixtyConnector.lastRun", new Date().toString());
              this.configuration.save();
            }

            this.postSendDataQualityRules().then(() => {
            });
          });


        });
      })
    });
  }

  postSendDataQualityProfiles(): PromiseLike<any> {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  }

  private _getSafeDateTimeString(): string {
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

    return `${year}-${month}-${day} ${hours}:${minutes}:${milliseconds}`;
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

// Send rule data and profile data over.
const runner = new CollibraConnector();
runner.retrieveAssets().then(() => {
  runner.sendDataQualityRules();
  runner.sendDataQualityProfiles();
})
