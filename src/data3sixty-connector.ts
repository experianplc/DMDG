import Connector from "./connector";
import * as tailored from "tailored";
import axios from "axios";
import * as EventEmitter from "events";
import produce from "immer";
import Promise from "bluebird";

const $ = tailored.variable();
const _ = tailored.wildcard();


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

  apiKey?: string;
  apiSecret?: string;
  fusionAttributeUid?: string;

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
  assetMetaDataToTechnologyAssetUuid?: MetaDataMap;

  constructor() {
    super();

    tailored.defmatch(
      tailored.clause([{
        Data3SixtyConnector: {
          apiKey: $
        }
      }], () => {
        throw(`You're missing an API key.
        You can find this {Data3SixtyInstance}/resource/my/apikey
        `)
      }, (apiKey: string) => !Boolean(apiKey)),

      tailored.clause([{
        Data3SixtyConnector: {
          apiSecret: $
        }
      }], () => {
        throw(`You're missing an API Secret.
          You can find this {Data3SixtyInstance}/resource/my/apikey
          `)
      }, (apiSecret: string) => !Boolean(apiSecret)),

      tailored.clause([{
        Data3SixtyConnector: {
          fusionAttributeUid: ""
        }
      }], (fusionAttributeUid: string) => {
        throw ("You're missing the fusionAttributeUid");
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

        // Later this will actually read from Redis in real time, and so
        // a brand new instantation of an object will be unnecssary, but for now
        // we'll just make a brand new object each time.
        this.assetMetaDataToTechnologyAssetUuid = {};
      }),

    )(this.configuration.connectorOptions);
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
      return axios.request({
        url: `${this.data3SixtyUrl}/api/v2/assets/${this.fusionAttributeUid}`,
        method: "GET",
        headers: {
          "Authorization": `${this.apiKey};${this.apiSecret}`
        },
        transformResponse: this.postRetrieveAssets.bind(this)
      });
    });
  };

  /*
   * After getting the assets do whatever clean-up you would like. 
   * This might be sending an email or another type of notification
   * to a system.
   */
  postRetrieveAssets(data: string): any {
    /*
     * For now we will just store everything in memory, but as the amount of
     * items we expect 'data' above to store increases we will need to eventually
     * move memory storage into something similar to redis, and query that, instead.
     *
     * If we go that route, we will also need to occassionally flush and rebuild 
     * the storage and/or remove unused keys for the purposes of efficency.
     */

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

          const Column   = normalizedProperties.Column;
          const Table    = normalizedProperties.Table;
          const Schema   = normalizedProperties.Schema;
          const Instance = normalizedProperties.Instance;

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
      }),
      tailored.clause([_], () => {
        // We want to throw an error if there are no actual
        // TechnologyAssets to query against. 
        throw "Items were not found in response";
      })
    )(JSON.parse(data));
  };

  /*
   * Before sending the Data Quality Rules do whatever clean-up you
   * would like. This might be checking to see if data quality rules exist
   * before retrieval.
   */
  preSendDataQualityRules(): PromiseLike<any>  {
    return new Promise((resolve: any, reject: any) => {
      resolve(null);
    });
  };

  /*
   * Send data quality rules to the system. The rules that are chosen to 
   * be sent will depend on the configuration options you have selected. 
   */
  sendDataQualityRules(): PromiseLike<any>  {
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
  postSendDataQualityRules(): PromiseLike<any>  {
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
