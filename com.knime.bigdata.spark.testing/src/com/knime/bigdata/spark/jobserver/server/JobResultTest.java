package com.knime.bigdata.spark.jobserver.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.StructField;
import org.junit.Test;

import com.knime.bigdata.spark.UnitSpec;
import com.knime.bigdata.spark.jobserver.server.JobResult;

/**
 *
 * @author dwk
 *
 */
public class JobResultTest extends UnitSpec {

    /**
     *
     */
    @Test
    public void emptyJobResultHasEmptyMessageAndTableAndNoModel() {
        final JobResult testObj = JobResult.emptyJobResult();
        assertEquals("empty message expected", "", testObj.getMessage());
        assertEquals("empty map of table schemas expected", 0, testObj.getTableNames().size());
        assertEquals("no model expected", null, testObj.getObjectResult());
    }

    /**
    *
    */
    @Test
    public void jobResultWithMsgHasMessageAndEmptyTableAndNoModel() {
        final JobResult testObj = JobResult.emptyJobResult().withMessage("my message");
        assertEquals("non-empty message expected", "my message", testObj.getMessage());
        assertEquals("empty map of table schemas expected", 0, testObj.getTableNames().size());
        assertEquals("no model expected", null, testObj.getObjectResult());
    }

    /**
   *
   */
    @Test
    public void jobResultWithTableHasNoMessageAndTableAndNoModel() {
        final JobResult testObj =
            JobResult.emptyJobResult().withTable(
                "key",
                DataType.createStructType(new StructField[]{DataType.createStructField("name", DataType.StringType,
                    false)}));
        assertEquals("empty message expected", "", testObj.getMessage());
        assertEquals("non-empty map of table schemas expected", 1, testObj.getTableNames().size());
        assertEquals("no model expected", null, testObj.getObjectResult());
    }

    /**
    *
    */
    @Test
    public void jobResultWithObjectHasNoMessageAndNoTableAndModel() {
        final JobResult testObj = JobResult.emptyJobResult().withObjectResult(new KMeansModel(null));
        assertEquals("empty message expected", "", testObj.getMessage());
        assertEquals("empty map of table schemas expected", 0, testObj.getTableNames().size());
        assertTrue("model expected", testObj.getObjectResult() != null);
    }

    /**
    *
    */
    @Test
    public void jobResultsWithAllFieldsSetToSameValueShouldGiveSameString() {
        final JobResult testObj1 =
            JobResult
                .emptyJobResult()
                .withObjectResult(new KMeansModel(null))
                .withMessage("xxx")
                .withTable(
                    "key",
                    DataType.createStructType(new StructField[]{DataType.createStructField("name", DataType.StringType,
                        false)}));
        final JobResult testObj2 =
            JobResult
                .emptyJobResult()
                .withMessage("xxx")
                .withObjectResult(new KMeansModel(null))
                .withTable(
                    "key",
                    DataType.createStructType(new StructField[]{DataType.createStructField("name", DataType.StringType,
                        false)}));

        assertEquals("equivalent string representation expected", testObj1.toString(), testObj2.toString());
    }

    /**
    *
    */
    @Test
    public void jobResultWithAllFieldsSetToStringAndBackShouldGiveComparableObject() {
        final JobResult testObj1 =
            JobResult
                .emptyJobResult()
                .withObjectResult(new KMeansModel(null))
                .withMessage("xxx")
                .withTable(
                    "key",
                    DataType.createStructType(new StructField[]{DataType.createStructField("name", DataType.StringType,
                        false)}))
                .withTable(
                    "key2",
                    DataType.createStructType(new StructField[]{
                        DataType.createStructField("name double", DataType.DoubleType, true),
                        DataType.createStructField("name bool", DataType.BooleanType, true)}));
        final String objStr = testObj1.toString();
        final JobResult testObj2 = JobResult.fromBase64String(objStr);

        assertEquals("equivalent string representation expected", testObj1.toString(), testObj2.toString());
    }

    /**
    *
    */
    @Test
    public void twoDifferentJobResultsShouldGiveDifferentStrings() {
        final JobResult testObj1 =
            JobResult
                .emptyJobResult()
                .withObjectResult(new KMeansModel(null))
                .withMessage("xxx")
                .withTable(
                    "key",
                    DataType.createStructType(new StructField[]{DataType.createStructField("name", DataType.StringType,
                        false)}))
                .withTable(
                    "key2",
                    DataType.createStructType(new StructField[]{
                        DataType.createStructField("name double", DataType.DoubleType, true),
                        DataType.createStructField("name bool", DataType.BooleanType, true)}));

        final JobResult testObj2 =
                JobResult
                    .emptyJobResult()
                    .withObjectResult(new KMeansModel(null))
                    .withMessage("xxx")
                    .withTable(
                        "key",
                        DataType.createStructType(new StructField[]{DataType.createStructField("name", DataType.StringType,
                            false)}));

        assertFalse("non-equivalent string representations expected", testObj1.toString().equals(testObj2.toString()));
    }

}
