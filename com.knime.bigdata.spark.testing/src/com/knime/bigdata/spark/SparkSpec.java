package com.knime.bigdata.spark;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.knime.core.node.CanceledExecutionException;

import com.knime.bigdata.spark.SparkPlugin;
import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.KNIMEConfigContainer;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.node.io.table.writer.Table2SparkNodeModel;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

/**
 *
 * @author dwk
 *
 */
public abstract class SparkSpec extends UnitSpec {

    private static Config origConfig = KNIMEConfigContainer.m_config;

    @SuppressWarnings("javadoc")
    protected static KNIMESparkContext CONTEXT_ID;

    final static String ROOT_PATH = ".." + File.separatorChar + "com.knime.bigdata.spark" + File.separator;

    /**
     * @return path to job jar (in main project)
     */
    protected static String getJobJarPath() {
        return SparkPlugin.getDefault().getPluginRootPath() + File.separatorChar + "resources" + File.separatorChar
            + "knimeJobs.jar";
    }

    /**
     * make sure that we do not connect to the server
     *
     * @throws GenericKnimeSparkException
     */
    @BeforeClass
    public static void beforeSuite() throws GenericKnimeSparkException {
        new SparkPlugin() {
            @Override
            public String getPluginRootPath() {
                return new File(ROOT_PATH).getAbsolutePath();
            }
        };

        KNIMEConfigContainer.m_config =
            KNIMEConfigContainer.m_config.withValue("unitTestMode", ConfigValueFactory.fromAnyRef("true"));
        //comment this out if you want to test the real server
        //use a dummy RestClient to be able to test things locally
        //        KNIMEConfigContainer.m_config =
        //                KNIMEConfigContainer.m_config.withValue("spark.jobServer", ConfigValueFactory.fromAnyRef("dummy"));

        CONTEXT_ID = KnimeContext.getSparkContext();

        //TODO: Upload the static jobs jar only if not exists
        JobControler.uploadJobJar(CONTEXT_ID, getJobJarPath());
    }

    /**
     * restore original configuration
     *
     * @throws Exception
     */
    @AfterClass
    public static void afterSuite() throws Exception {
        KNIMEConfigContainer.m_config = origConfig;
        //        KnimeContext.destroySparkContext(CONTEXT_ID);
        //        //need to wait a bit before we can actually test whether it is really gone
        //        Thread.sleep(200);
        //        // TODO - what would be the expected status?
        //        assertTrue("context status should NOT be OK after destruction",
        //            KnimeContext.getSparkContextStatus(CONTEXT_ID) != JobStatus.OK);

    }

    protected static final Object[][] TEST_TABLE = new Object[][]{new Object[]{1, true, 3.2d, "my string"},
        new Object[]{2, false, 3.2d, "my string"},
        new Object[]{3, true, 38d, "my other string"},
        new Object[]{4, false, 34.2d, "my other string"}};
    
    protected static final Object[][] MINI_IRIS_TABLE = new Object[][]{
    	{5.1, 3.5, 1.4, 0.2, "Iris-setosa"}, 
    	{4.9, 3.0, 1.4, 0.2, "Iris-setosa"}, 
    	{4.7, 3.2, 1.3, 0.2, "Iris-versicolor"},
        {4.6, 3.1, 1.5, 0.2, "Iris-virginica"}};
    
    /**
     * @param contextName
     * @param resTableName
     * @return
     * @throws GenericKnimeSparkException
     * @throws CanceledExecutionException
     */
    public static String importTestTable(final KNIMESparkContext contextName, final Object[][] aTable, final String resTableName)
        throws GenericKnimeSparkException, CanceledExecutionException {
        String params =
                Table2SparkNodeModel.paramDef(aTable, resTableName);
        String jobId = JobControler.startJob(contextName, ImportKNIMETableJob.class.getCanonicalName(), params.toString());

        JobControler.waitForJobAndFetchResult(contextName, jobId, null);
        return jobId;
    }

}