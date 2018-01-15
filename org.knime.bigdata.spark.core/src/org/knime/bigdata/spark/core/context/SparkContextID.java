/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
package org.knime.bigdata.spark.core.context;

import java.net.URI;
import java.net.URISyntaxException;

import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;

/**
 * Uniquely identifies a {@link SparkContext} in the KNIME Extension for Apache Spark.
 *
 * @see SparkContext
 * @see SparkContextManager
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkContextID {

    private final static String CFG_CONTEXT_ID = "sparkContextID";

    private final String m_stringID;

    /**
     * Create a new ID backed by the given String.
     *
     * @param stringID
     */
    public SparkContextID(final String stringID) {
        m_stringID = stringID;
    }

    @Override
    public String toString() {
        return m_stringID;
    }

    @Override
    public boolean equals(final Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof SparkContextID)) {
            return false;
        }
        return ((SparkContextID)other).m_stringID.equals(m_stringID);
    }

    /**
     * @return a nice String representation of the Spark cntext information
     */
    public String toPrettyString() {
        if (this.equals(SparkContextManager.getDefaultSparkContextID())) {
            return SparkContextManager.getDefaultSparkContext().getID().toPrettyString();
        } else {
            URI uri = URI.create(m_stringID);
            StringBuilder b = new StringBuilder();
            b.append("Spark Jobserver Context ");
            b.append(String.format("(Host and Port: %s:%d, ", uri.getHost(), uri.getPort()));
            b.append(String.format("Context Name: %s)", uri.getPath().substring(1)));
            return b.toString();
        }
    }

    @Override
    public int hashCode() {
        return m_stringID.hashCode();
    }

    /**
     * @param config {@link SparkContextConfig} to read from
     * @return {@link SparkContextID}
     */
    public static SparkContextID fromContextConfig(final SparkContextConfig config) {
        return fromConnectionDetails(config.getJobServerUrl(), config.getContextName());
    }

    /**
     * @param jobServerUrl job server url
     * @param contextName context name
     * @return {@link SparkContextID}
     */
    public static SparkContextID fromConnectionDetails(final String jobServerUrl, final String contextName) {
        try {
            final URI url = new URI(jobServerUrl);
            return new SparkContextID(String.format("jobserver://%s:%d/%s", url.getHost(), url.getPort(), contextName));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URL: " + jobServerUrl);
        }
    }

    /**
     * @param conf {@link ConfigRO} to read from
     * @return {@link SparkContextID}
     * @throws InvalidSettingsException
     */
    public static SparkContextID fromConfigRO(final ConfigRO conf) throws InvalidSettingsException {
        return new SparkContextID(conf.getString(CFG_CONTEXT_ID));
    }

    /**
     * @param configWO
     */
    public void saveToConfigWO(final ConfigWO configWO) {
        configWO.addString(CFG_CONTEXT_ID, m_stringID);
    }
}