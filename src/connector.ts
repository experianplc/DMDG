import fs from "fs";
import tailored from "tailored";
import path from "path";

/* 
 * The Connector is a class that wil limplement the logic needed to do the following four things:
 * - Retrieve any necessary assets from the [source], defined as the Governance Center's needed
 *   assets
 * - Do any processing after retrival via sourceCallback
 * - Post Pandora/Aperture data quality rules to [target], defined as the Governance Center's
 *   location for where Pandora Data Quality Rules should be stored
 * - Do any processing after posting of data quality rules
 */

const $ = tailored.variable();

export default abstract class Connector {

  configuration: any;
  data3SixtyUrl?: string;

  constructor() {
    // TODO: Once the project is done there will need to be some thought on where this should be
    // put.
    const connectorConfig = fs.readFileSync(path.resolve(__dirname, "../connector-config.json"), "utf8");
    this.configuration = JSON.parse(connectorConfig);

    tailored.defmatch(
      tailored.clause([$], () => {
        throw `You're missing a configuration file. 
        Please create a configuration named 'connector-config.json' 
        in the root of this project.`;
      }, (config: object) => !Boolean(config)),

      tailored.clause([{
        connectorOptions: {
          data3SixtyUrl: ""
        }
      }], () => {
        throw `You're missing a URL for your data3Sixty instance.
          It should look something like name.data3Sixty.com`;
      }),

      tailored.clause([{
        connectorOptions: {
          data3SixtyUrl: $,
        }
      }], (data3SixtyUrl: string) => {
        this.data3SixtyUrl = data3SixtyUrl;
      })
    )(this.configuration);
  }

  /*
   * Before getting assets from the source (typically 
   * a data governance center) do whatever clean-up you would like.
   * This might be checking authentication, for example.
   */
  abstract preRetrieveAssets(): Promise<any> 

  /*
   * Get assets from a source. These assets might be Technology Assets
   * from Data3Sixty or getting a variety of assets for joining from 
   * another system.
   */
  abstract retrieveAssets(): Promise<any> 

  /*
   * After getting the assets do whatever clean-up you would like. 
   * This might be sending an email or another type of notification
   * to a system.
   */
  abstract postRetrieveAssets(): Promise<any> 

  /*
   * Before sending the Data Quality Rules do whatever clean-up you
   * would like. This might be checking to see if data quality rules exist
   * before retrieval.
   */
  abstract preSendDataQualityRules(): Promise<any> 

  /*
   * Send data quality rules to the system. The rules that are chosen to 
   * be sent will depend on the configuration options you have selected. 
   */
  abstract sendDataQualityRules(): Promise<any> 

  /*
   *
   * After sending the data quality rules, do whatever clean-up 
   * you would like. This might be sending an email or another 
   * type of notification to a system.
   */
  abstract postSendDataQualityRules(): Promise<any> 
}
