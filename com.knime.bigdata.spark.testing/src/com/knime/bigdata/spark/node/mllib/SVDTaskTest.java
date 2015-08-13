package com.knime.bigdata.spark.node.mllib;

import static org.junit.Assert.assertEquals;

import javax.annotation.Nullable;

import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.jobs.SVDJob;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class SVDTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputTableName, final Integer[] aNumericColIdx,
	        final boolean aComputeU, final int aK, final double aRCond, final String aVMatrix,
	        @Nullable final String aUMatrix) {
		return SVDTask.paramsAsJason(aInputTableName, aNumericColIdx, aComputeU, aK, aRCond, aVMatrix, aUMatrix);
	}
	
    @Test
    public void ensureThatAllRequiredParametersAreSet() throws Throwable {
        SVDTask testObj = new SVDTask(null, "inputRDD", new Integer[] {0,1}, true, 2, 0.4d, "v", "u");
        final String params = testObj.paramsAsJason();
        JobConfig config = new JobConfig(ConfigFactory.parseString(params));
        
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            new SVDJob().validate(config));
    }

    @Test
    public void verifyThatSVDJobStoresResultMatricesAsNamedRDDAndReturnsVector() throws Throwable {
        KNIMESparkContext context = KnimeContext.getSparkContext();
        ImportKNIMETableJobTest.importTestTable(context, MINI_IRIS_TABLE, "tab1");

        SVDTask testObj = new SVDTask(context, "tab1", new Integer[] {0,1,2,3}, true, 4, 0.000001d, "v", "u");

        double[] v = testObj.execute(null);
        
        assertEquals("expected one value per column", 4, v.length);
        
        Object[][] expected = new Object[4][];
        for (int i = 0; i < expected.length; i++) {
            expected[i] =
                new Object[]{ImportKNIMETableJobTest.TEST_TABLE[i][0], ImportKNIMETableJobTest.TEST_TABLE[i][1],
                    ImportKNIMETableJobTest.TEST_TABLE[i][2], ImportKNIMETableJobTest.TEST_TABLE[i][3],
                    ImportKNIMETableJobTest.TEST_TABLE[i][0], ImportKNIMETableJobTest.TEST_TABLE[i][1],
                    ImportKNIMETableJobTest.TEST_TABLE[i][2], ImportKNIMETableJobTest.TEST_TABLE[i][3]};
        }
        
        //not sure what else to check here....
        Object[][] arrayResV = fetchResultTable(context, "v",4);
        Object[][] arrayResU = fetchResultTable(context, "u",4);

    }


}
