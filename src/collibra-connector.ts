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

interface DomainArguments {
  name: string;
  communityId: string;
  typeId: string;
  description?: string;
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

  // ID in Collibra for the community
  communityId: string;

  // Maps Domain names to IDs
  domainNameToId: any;

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

    this.communityId = "";
    this.domainNameToId = {};
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
    // Move to config
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

      // Create community if non existent
      this.getOrCreateCommunity(communityName, communityDescription).then(() => {
        domains.forEach((domain) => {
          this.getOrCreateDomain({
            name: domain.name,
            communityId: this.communityId,
            typeId: domain.typeId,
            description: domain.description
          })
        });
      }).then(() => {
        axios.request({ 
          url: `http://${this.odbcUrl}/query`,
          method: "POST",
          headers: { "Content-Type": "application/json", "Cookie": this.cookie },
          data: {
            sql: `SELECT * FROM "RULES"`,
          }
        }).then((ruleData: any) => {
          // get rules
          // for each rule:
          // Create Database asset and add to Data Asset Domain if nonexistent
          //  Create Table asset and add to Data Asset Domain if nonexistent
          //  Create column assets and add to Data Asset Domain if nonexistent
          //  Create rules and add to Rulebook if nonexistent
          //  Create Data Quality Metrics and add them to Governance Asset Domain if nonexistent
        });

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
    return new Promise((resolve: any) => {
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
          console.log("Sign in successful.");
          this.sessionToken = data["csrfToken"];
          this.cookie = response.headers["set-cookie"].join(" ");
          resolve(data);
        }
      });
    })
  }

  private getOrCreateCommunity(name: string, description: string): PromiseLike<any> {
    if (this.communityId) {
      console.log("Continuing on with Governance Asset Domain Name creation.")
      return new Promise((resolve: any) => {
        resolve(this.communityId);
      })
    }

    return new Promise((resolve: any, reject: any) => {
      console.log("Community ID not saved. Checking to see if it exists in the Governance Center...");
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
          console.log(`No community with name '${name}' found.`);
          console.log("Creating community...");
          axios.request({ 
            url: `${this.url}/rest/2.0/communities`,
            method: "POST",
            headers: { "Content-Type": "application/json", "Cookie": this.cookie },
            data: {
              name,
              description,
            }
          }).then(({ data }) => {
            console.log(`Community with name ${data.name} created successfully.`);
            console.log(data);
            this.communityId = data["ID"];
            resolve(this.getOrCreateCommunity(name, description));
          }).catch((err) => reject(err));
        } else {
          console.log("Community ID found in governance center. Saving locally...");
          const result: CommunityResult  = data.results[0];
          this.communityId = result.id;
          resolve(this.getOrCreateCommunity(name, description));
        }
      }).catch((err) => { console.log(err); reject(err) });    
    });
  }

  private getOrCreateDomain(args: DomainArguments): PromiseLike<any> {
    let { name, communityId, typeId, description } = args;

    if (!communityId) { 
      throw "Something went wrong. A community is not specified.";
    }

    if (!typeId) {
      throw "Something went wrong. A typeId is not specified.";
    }

    if (this.domainNameToId[name]) {
      console.log(`Id for domain '${name}' saved. Continuing...`)
      return new Promise((resolve: any, reject: any) => {
        resolve(this.domainNameToId[name]);
      });
    }

    /* In effect, this checks to see if the Domain exists. 
     * If it does exist, we take its value and save it, continuing.
     * If it does not exist we create it and save its value, continuing.
     */
    return new Promise((resolve: any, reject: any) => {
      axios.request({ 
        url: `${this.url}/rest/2.0/communities`,
        method: "GET",
        headers: { "Content-Type": "application/json", "Cookie": this.cookie },
        params: {
          name: name,
          communityId: this.communityId,
        }
      }).then((response) => {
        const data: DomainGetResponse = response.data;

        if (data.total === 0) {
          console.log(`No domain with name '${name}' found. Creating now...`);
          axios.request({ 
            url: `${this.url}/rest/2.0/domains`,
            method: "POST",
            headers: { "Content-Type": "application/json", "Cookie": this.cookie },
            data: {
              name,
              communityId: this.communityId,
              typeId, 
              description,
            }
          }).then((response: any) => {
            console.log(`Domain with name ${response.name} created successfully.`);
            this.domainNameToId[name] = response.ID;
            resolve(this.getOrCreateDomain({name, communityId, typeId, description}))
          })

        } else {
          const domainResult: DomainResult = data.results[0];
          this.domainNameToId[name] = domainResult.id;
          resolve(this.getOrCreateDomain({name, communityId, typeId, description}))
        }
      })
    }) 
  };


};

// Send rule data and profile data over.
const runner = new CollibraConnector();
 runner.retrieveAssets().then(() => {
  runner.sendDataQualityRules();
  runner.sendDataQualityProfiles();
});
