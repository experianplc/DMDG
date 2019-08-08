import { CollibraConnector } from "../../lib/collibra-connector";
import intern from "intern";

const { expect, assert } = intern.getPlugin("chai");
const { registerSuite } = intern.getInterface("object");

registerSuite("CollibraConnector tests", {

  tests: {
    "CollibraConnector can be instantiated"() {
      let connector = new CollibraConnector();
      assert.equal(Boolean(connector.url), true, "Collibra URL exists");
      assert.equal(Boolean(connector.odbcUrl), true, "ODBC URL exists");
    },

    "CollibraConnector can retreieve technology assets"() {
      let connector = new CollibraConnector();

      return connector.retrieveAssets().then((response) => {
        assert.equal(Boolean(response.csrfToken), true, "A response is present");
      });
    },

    "CollibraConnector can post data quality rule results"() {
      const finished = this.async();
      let connector = new CollibraConnector();
      connector.retrieveAssets().then(() => {
        connector.sendDataQualityRules().then((response) => {
          finished.resolve(() => {
            assert.equal(response, "Import complete", "Import complete");
          });
        })
      });
    },

    "CollibraConnector can post data quality profile results"() {
      const finished = this.async();
      let connector = new CollibraConnector();
      connector.retrieveAssets().then(() => {
        connector.sendDataQualityProfiles().then((response) => {
          finished.resolve(() => {
            assert.equal(response, "Import complete", "Import complete");
          });
        })
      });
    }
  }

});
