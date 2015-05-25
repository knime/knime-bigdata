package com.knime.bigdata.spark.testing;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.knime.bigdata.spark.jobserver.client.KnimeConfigContainer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

/**
 *
 * @author dwk
 *
 */
public abstract class UnitSpec {

    private static Config origConfig = KnimeConfigContainer.m_config;

    /**
     * make sure that we do not connect to the server
     */
    @BeforeClass
    public static void beforeSuite() {
        //comment this out if you want to test the real server
        //use a dummy RestClient to be able to test things locally
        KnimeConfigContainer.m_config =
            KnimeConfigContainer.m_config.withValue("spark.jobServer", ConfigValueFactory.fromAnyRef("dummy"));
    }

    /**
     * restore original configuration
     */
    @AfterClass
    public static void afterSuite() {
        KnimeConfigContainer.m_config = origConfig;
    }

}