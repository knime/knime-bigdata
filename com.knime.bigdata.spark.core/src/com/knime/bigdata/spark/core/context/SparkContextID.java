/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Mar 2, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.context;

import java.net.URI;
import java.net.URISyntaxException;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;

/**
 *
 *
 * @author bjoern
 */
public class SparkContextID {

    private final static String CFG_CONTEXT_ID = "sparkContextID";

    private final String stringID;

    public SparkContextID(final String stringID) {
        this.stringID = stringID;
    }

    @Override
    public String toString() {
        return stringID;
    }

    @Override
    public boolean equals(final Object other) {
        if (other.getClass() != getClass()) {
            return false;
        } else {
            return ((SparkContextID)other).stringID.equals(stringID);
        }
    }

    public String toPrettyString() {
        if (this.equals(SparkContextManager.getDefaultSparkContextID())) {
            return SparkContextManager.getDefaultSparkContext().getID().toPrettyString();
        } else {
            URI uri = URI.create(stringID);
            StringBuilder b = new StringBuilder();
            b.append("Spark Jobserver Context ");
            b.append(String.format("(Host and Port: %s:%d, ", uri.getHost(), uri.getPort()));
            b.append(String.format("Context Name: %s)", uri.getPath().substring(1)));
            return b.toString();
        }
    }

    @Override
    public int hashCode() {
        return stringID.hashCode();
    }


    public static SparkContextID fromConnectionDetails(final String jobManagerUrl, final String contextName) {
        try {
            final URI url = new URI(jobManagerUrl);
            return new SparkContextID(String.format("jobserver://%s:%d/%s", url.getHost(), url.getPort(), contextName));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URL: " + jobManagerUrl);
        }
    }

    public static SparkContextID fromConfigRO(final ConfigRO conf) throws InvalidSettingsException {
        return new SparkContextID(conf.getString(CFG_CONTEXT_ID));
    }

    public void saveToConfigWO(final ConfigWO configWO) {
        configWO.addString(CFG_CONTEXT_ID, stringID);
    }
}
