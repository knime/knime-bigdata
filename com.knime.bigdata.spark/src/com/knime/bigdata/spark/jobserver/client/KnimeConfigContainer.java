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
public class KnimeConfigContainer {

    /**
     * mutable configuration object
     */
	public static Config m_config = ConfigFactory.load();
}