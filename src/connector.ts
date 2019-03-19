import * as fs from "fs";
import * as tailored from "tailored";
import * as path from "path";

/* 
 * The Connector is a class that will implement the logic needed to do the following four things:
 * - (1) Retrieve any necessary assets from the [source], defined as the Governance Center's needed
 *   assets. 
 * - (2) Do any processing after retrival via sourceCallback
 * - (3) Post Pandora/Aperture data quality rules to [target], defined as the Governance Center's
 *   location for where Pandora Data Quality Rules should be stored
 * - (4) Do any processing after posting of data quality rules. For example, sending notifications
 */

const $ = tailored.variable();

export default abstract class Connector {

  /*
   * Before getting assets from the source (typically 
   * a data governance center) do whatever clean-up you would like.
   * This might be checking authentication, for example.
   */
  abstract preRetrieveAssets(): PromiseLike<any> 

  /*
   * Get assets from a source. These assets might be Technology Assets
   * from Data3Sixty or getting a variety of assets for joining from 
   * another system.
   */
  abstract retrieveAssets(): PromiseLike<any> 

  /*
   * After getting the assets do whatever clean-up you would like. 
   * This might be sending an email or another type of notification
   * to a system.
   */
  abstract postRetrieveAssets(...data: any[]): any

  /*
   * Before sending the Data Quality Rules do whatever clean-up you
   * would like. This might be checking to see if data quality rules exist
   * before retrieval.
   */
  abstract preSendDataQualityRules(): PromiseLike<any> 

  /*
   * Send data quality rules to the system. The rules that are chosen to 
   * be sent will depend on the configuration options you have selected. 
   */
  abstract sendDataQualityRules(): PromiseLike<any> 

  /*
   *
   * After sending the data quality rules, do whatever clean-up 
   * you would like. This might be sending an email or another 
   * type of notification to a system.
   */
  abstract postSendDataQualityRules(...data: any[]): PromiseLike<any> 
}
