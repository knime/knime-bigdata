package com.knime.bigdata.spark.testing;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.knime.bigdata.spark.SparkPlugin;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.KNIMEConfigContainer;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.Config;

/**
 *
 * @author dwk
 *
 */
public abstract class SparkSpec extends UnitSpec {

    private static Config origConfig = KNIMEConfigContainer.m_config;

    protected static KNIMESparkContext CONTEXT_ID;

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
                return new File(".").getAbsolutePath();
            }
        };
        //comment this out if you want to test the real server
        //use a dummy RestClient to be able to test things locally
//        KNIMEConfigContainer.m_config =
//                KNIMEConfigContainer.m_config.withValue("spark.jobServer", ConfigValueFactory.fromAnyRef("dummy"));

        CONTEXT_ID = KnimeContext.getSparkContext();
    }

    /**
     * restore original configuration
     *
     * @throws Exception
     */
    @AfterClass
    public static void afterSuite() throws Exception {
        KNIMEConfigContainer.m_config = origConfig;
        KnimeContext.destroySparkContext(CONTEXT_ID);
        //need to wait a bit before we can actually test whether it is really gone
        Thread.sleep(200);
        // TODO - what would be the expected status?
        assertTrue("context status should NOT be OK after destruction",
            KnimeContext.getSparkContextStatus(CONTEXT_ID) != JobStatus.OK);

    }

}