package com.knime.bigdata.spark.testing;

import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.KnimeConfigContainer;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.typesafe.config.Config;

/**
 *
 * @author dwk
 *
 */
public abstract class UnitSpec {

    private static Config origConfig = KnimeConfigContainer.m_config;

    protected static String contextName;

    /**
     * make sure that we do not connect to the server
     *
     * @throws GenericKnimeSparkException
     */
    @BeforeClass
    public static void beforeSuite() throws GenericKnimeSparkException {
        //comment this out if you want to test the real server
        //use a dummy RestClient to be able to test things locally
        //KnimeConfigContainer.m_config =
        //    KnimeConfigContainer.m_config.withValue("spark.jobServer", ConfigValueFactory.fromAnyRef("dummy"));

        contextName = KnimeContext.getSparkContext();
    }

    /**
     * restore original configuration
     *
     * @throws Exception
     */
    @AfterClass
    public static void afterSuite() throws Exception {
        KnimeConfigContainer.m_config = origConfig;
        try {
            KnimeContext.destroySparkContext(contextName);
        } finally {
            KnimeContext.destroySparkContext(contextName);
            //need to wait a bit before we can actually test whether it is really gone
            Thread.sleep(200);
        }
        // TODO - what would be the expected status?
        assertTrue("context status should NOT be OK after destruction",
            KnimeContext.getSparkContextStatus(contextName) != JobStatus.OK);

    }

}