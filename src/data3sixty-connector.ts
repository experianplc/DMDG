import Connector from "./connector";
import tailored from "tailored";
import axios from "axios";

const $ = tailored.variable();
const _ = tailored.wildcard();

export class Data3SixtyConnector extends Connector {

  apiKey?: string;
  apiSecret?: string;
  fusionAttributeUid?: string;

  constructor() {
    super();

    tailored.defmatch(
      tailored.clause([{
        Data3SixtyConnector: {
          apiKey: $
        }
      }], () => {
        throw `You're missing an API key.
        You can find this {Data3SixtyInstance}/resource/my/apikey
        `
      }, (apiKey: string) => !Boolean(apiKey)),

      tailored.clause([{
        Data3SixtyConnector: {
          apiSecret: $
        }
      }], () => {
        throw `You're missing an API Secret.
        You can find this {Data3SixtyInstance}/resource/my/apikey
          `
      }, (apiSecret: string) => !Boolean(apiSecret)),

      tailored.clause([{
        Data3SixtyConnector: {
          apiKey: $,
          apiSecret: $,
          fusionAttributeUid: ""
        }
      }], (apiKey: string, apiSecret: string) => {
        this.apiKey = apiKey;
        this.apiSecret = apiSecret;

        axios.get(`${this.data3SixtyUrl}/api/v2/assets/types`, {
          method: "GET",
          headers: {
            "Authorization": `${this.apiKey};${this.apiSecret}`
          }
        }).then((response) => {
          let assignFusisonAttributeUid = tailored.defmatch(
            tailored.clause([{ 
              uid: $, 
              Class: {
                Name: "Fusion Attribute"
              }
            }], (fusionAttributeUid: string) => { 
              // TODO: Handle the case where there are multiple fusionAttributeUIDs 
              this.fusionAttributeUid = fusionAttributeUid;
            }),

            tailored.clause([_], () => { return })
          );

          response.data.forEach((data: any) => assignFusisonAttributeUid(data))
        })
      }),

      tailored.clause([{
        Data3SixtyConnector: {
          apiKey: $,
          apiSecret: $,
          fusionAttributeUid: $,
        }
      }], (apiKey: string, apiSecret: string, fusionAttributeUid: string) => {
        this.apiKey = apiKey;
        this.apiSecret = apiSecret;
        this.fusionAttributeUid = fusionAttributeUid;
      }),

    )(this.configuration.connectorOptions);
  }

  /*
   * Before getting assets from the source (typically 
   * a data governance center) do whatever clean-up you would like.
   * This might be checking authentication, for example.
   */
  preRetrieveAssets(): Promise<any> {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  }

  /*
   * Get assets from a source. These assets might be Technology Assets
   * from Data3Sixty or getting a variety of assets for joining from 
   * another system.
   */
  retrieveAssets(): Promise<any>  {
    axios.request({
      method: "GET",
      headers: {
        "Authorization": `${this.apiKey};${this.apiSecret}`
      }
    }).then((response) => {
      console.log(response);
    })

    // Make the request 
    // Download assets
    // Return a promise that resolves to have the assets
    return new Promise((resolve: any, reject: any) => {
    });
  }

  /*
   * After getting the assets do whatever clean-up you would like. 
   * This might be sending an email or another type of notification
   * to a system.
   */
  postRetrieveAssets(): Promise<any> {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  }

  /*
   * Before sending the Data Quality Rules do whatever clean-up you
   * would like. This might be checking to see if data quality rules exist
   * before retrieval.
   */
  preSendDataQualityRules(): Promise<any>  {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  }

  /*
   * Send data quality rules to the system. The rules that are chosen to 
   * be sent will depend on the configuration options you have selected. 
   */
  sendDataQualityRules(): Promise<any>  {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  }

  /*
   *
   * After sending the data quality rules, do whatever clean-up 
   * you would like. This might be sending an email or another 
   * type of notification to a system.
   */
  postSendDataQualityRules(): Promise<any>  {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  }
}
