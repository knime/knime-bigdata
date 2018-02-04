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

import org.knime.bigdata.spark.core.port.context.JobServerSparkContextConfig;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;

/**
 * Uniquely identifies a {@link SparkContext} in the KNIME Extension for Apache Spark. This class is a simple wrapper
 * around a String ID that has the shape of a URL: scheme://host[:port][/path].
 *
 * @see SparkContext
 * @see SparkContextManager
 * @author Bjoern Lohrmann, KNIME GmbH
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
            return SparkContextProviderRegistry.getSparkContextProvider(getScheme()).toPrettyString(this);
        }
    }

    @Override
    public int hashCode() {
        return m_stringID.hashCode();
    }

    /**
     * @param config {@link JobServerSparkContextConfig} to read from
     * @return {@link SparkContextID}
     */
    public static SparkContextID fromContextConfig(final JobServerSparkContextConfig config) {
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

    public String getScheme() {
        try {
            final URI url = new URI(m_stringID);
            return url.getScheme();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URL: " + m_stringID);
        }

    }
}
