package com.knime.bigdata.spark.jobserver.client;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * configuration container (reads values from application.conf file, located in
 * src/main/resources)
 *
 * mutable for unit testing, application program should not change values
 *
 * @author dwk
 *
 */
public class KNIMEConfigContainer {

    /**
     * mutable configuration object
     */
	public static Config m_config = ConfigFactory.load();

	/**
	 * global context name - the job server currently supports only one context!
	 */
    public final static String CONTEXT_NAME;

    static {
        CONTEXT_NAME =
            KNIMEConfigContainer.m_config.hasPath("spark.contextName") ? KNIMEConfigContainer.m_config
                .getString("spark.contextName") : "knime";
    }
    //  + "."+ KNIMEConfigContainer.m_config.getString("spark.userName");

}
