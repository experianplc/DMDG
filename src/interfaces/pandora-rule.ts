/**
 * All of the fields shown below, if not included by default in Pandora or Data Studio
 * can be included by including them into [[CollibraEnvironmentVariables.COLLIBRA_ATTRIBUTE_KEY]].
 *
 * So, for example, if the COLLIBRA_ATTRIBUTE_KEY is "Description", then by including
 * "SCORE=100;" in the Description the score will be parsed from that if not included as a column.
 */
interface PandoraRule {
  /**
   * Maps to "Name", "Display Name" in the Collibra Data Quality Metric
   */
  "NAME": string;
  "SCORE": string;
  "MEASURE": string;
  "PASSED MEASURE": string;
  "FAILED MEASURE": string;
  "RULE CATEGORY": string;
  "DESCRIPTION": string;
  "INCLUDED": string;
  "FILTER": string;
  /**
   * Maps to "Rows Passed", "Conformity Score" in the Collibra Data Quality Metric
   * Maps to "Passing Fraction" when calculated with [[ROWS CONSIDERED"]]
   */
  "ROWS PASSED": string;
  /**
   * Maps to "Rows Failed", "Non Conformity Score" in the Collibra Data Quality Metric"
   */
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
  /**
   * Maps to "Loaded Rows" in the Collibra Data Quality Metric
   * Maps to "Passing Fraction" when calculated with [[ROWS PASSED"]]
   */
  "ROWS CONSIDERED": string;
  "ROWS IGNORED": string;
  "PASSED": string;
  "FAILED": string;
  "CONSIDERED": string;
  "IGNORED": string;
  "CATEGORY": string;
  "TABLE CREATED": string;
  /**
   * Maps to "Last Sync Date" in the Collibra Data Quality Metric
   */
  "LAST VALIDATED": string;
  "CONNECTION STRING": string;
  /**
   * In Collibra a Column Asset is created from this
   */
  "EXTERNAL COLUMN NAME": string;
  /**
   * In Collibra a Database Asset is created from this
   */
  "EXTERNAL DATABASE": string;
  "EXTERNAL SCHEMA": string;
  /**
   * In Collibra the Data Asset Domain is created from this
   */
  "EXTERNAL SERVER": string;
  /**
   * In Collibra a Table Asset is created from this
   */
  "EXTERNAL TABLE NAME": string;
  "FAIL RANGE": string;
  "FAILED SCORE": string;
  "LATEST TABLE VERSION": string;

  /**
   * Maps to the "Threshold" attribute in the Collibra Data Quality Metric
   */
  "PASS RANGE": string;

  /**
   * Maps to the "Result" attribute in the Collibra Data Quality Metric
   */
  "RESULT": string;
  "RULE CATEGORY ID": string;
  "RULE THRESHOLD": string;

  /**
   * Not included by default. Must be included in the given
   * [[CollibraEnvironmentVariables.COLLIBRA_ATTRIBUTE_KEY]].
   *
   * Maps to the "Description" attribute in the Collibra Data Quality Metric
   */
  "RULE DESCRIPTION"?: string;

  /**
   * Not included by default. Must be included in the given
   * [[CollibraEnvironmentVariables.COLLIBRA_ATTRIBUTE_KEY]].
   *
   * Maps to the "Descriptive Example" attribute in the Collibra Data Quality Metric
   */
  "EXAMPLE"?: string;
}
