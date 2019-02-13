/*
 *
 * In general the integration from Pandora/Aperture Data Studio should roughly work as follows:
 *
 * 1. (Optional) Get any assets from a [source], as configured (with a configured ingestion scheme).
 * 2. Aperture/Pandora rules are retrieved via ODBC (DSN)
 * 3. The rules are sent with a preconfigured scheme to a [target]
 *
 * The above scheme will be done for Pandora <-> Data3Sixty, but should be able to be extended
 * for any arbitrary integration.
 */


// Start a server
function startServer() {
  // Server should poll for new assets from [source] at a configurable frequency
  //pollAssetsFrom(source).then((assets) => {
    // Once the assets have been retrieved there can be a callback
  //dataQualityCallback(assets);

    // Once the callback has been completed the data quality rules should be 
    // ingested

  //getDataQualityRules(source).then((dataQualityRules) => {
      // (Optional) Reconcile DQ rules with mapping table
      // Send rules to target
  //sendRulesTo(target);
  // })
  //}); 
}
