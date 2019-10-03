interface PandoraProfile {
  /**
   * Maps to "Row Count" in the Column Asset
   */
  "ROW COUNT": string;

  /**
   * Maps to "Empty Values Count" in the Column Asset
   * In Data Studio NULL COUNT will be used instead.
   */
  "BLANK COUNT": string;

  /**
   * Maps to "Number of distinct values" in the Column Asset
   */
  "UNIQUE COUNT": string;

  /**
   * Maps to "Standard Deviation" in the Column Asset
   */
  "DOMINANT DATATYPE": string;

  "NATIVE TYPE": string;

  /**
   * Maps to "Standard Deviation" in the Column Asset
   */
  "STANDARD DEVIATION OF VALUES": string;

  /**
   * Maps to "Mode" in the Column Asset
   */
  "MOST COMMON VALUE": string;

  /**
   * Maps to "Minimum Value" in the Column Asset
   */
  "MINIMUM": string;

  /**
   * Maps to "Minimum Text Length" in the Column Asset
   */
  "ALPHANUMERIC MIN LENGTH": string;

  /**
   * Maps to "Mean" in the Column Asset
   */
  "AVERAGE": string;

  /**
   * Maps to "Maximum Value" in the Column Asset
   */
  "MAXIMUM": string;

  /**
   * Maps to Maximum Text Length in the Column Asset
   */
  "ALPHANUMERIC MAX LENGTH": string;

  /**
   * Maps to Is Primary Key in the Column Asset
   */
  "KEY CHECK": string;

  /**
   * Maps to "Is Nullable" in the Column Asset
   */
  "DOCUMENTED NULLABLE": string;

  /**
   * Maps to "Column Postion" in the Column Asset
   */
  "POSITION": string;

  /**
   * Maps to "Original Name" in the Column Asset
   */
  "NAME": string;

  /**
   * In Collibra a Column Asset is created from this
   */
  "EXTERNAL NAME": string; 

  /**
   * In Collibra a Database Asset is created from this
   */
  "EXTERNAL DATABASE": string;
  "EXTERNAL SCHEMA": string;
  /**
   * In Collibra a Data Asset Domain is created from this
   */
  "EXTERNAL SERVER": string;
  /**
   * In Collibra a Table Asset is created from this
   */
  "TABLE EXTERNAL NAME": string;
}
