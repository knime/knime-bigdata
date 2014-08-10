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
 *   Created on 06.05.2014 by thor
 */
package com.knime.bigdata.hive.node.connector;

import org.knime.base.node.io.database.connection.util.DefaultDatabaseConnectionSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.config.Config;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;
import org.knime.core.node.workflow.CredentialsProvider;

/**
 * Settings for the Hive connector node.
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 */
class HiveConnectorSettings extends DefaultDatabaseConnectionSettings {
    private boolean m_httpMode;

    HiveConnectorSettings() {
        setPort(10000);
        setRowIdsStartWithZero(true);
        setRetrieveMetadataInConfigure(false);
    }

    /**
     * Returns whether the Hive connection should be established via HTTP ("http mode") or if the native protocol should
     * be used (default).
     *
     * @return <code>true</code> if http mode should be used, <code>false</code> if the native mode should be used
     */
    public boolean isHttpMode() {
        return m_httpMode;
    }

    /**
     * Sets whether the Hive connection should be established via HTTP ("http mode") or if the native protocol should be
     * used.
     *
     * @param b <code>true</code> if http mode should be used, <code>false</code> if the native mode should be used
     */
    public void setHttpMode(final boolean b) {
        m_httpMode = b;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveConnection(final ConfigWO settings) {
        super.saveConnection(settings);

        Config hiveConfig = settings.addConfig("hive-connection");
        hiveConfig.addBoolean("http-mode", m_httpMode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateConnection(final ConfigRO settings, final CredentialsProvider cp)
        throws InvalidSettingsException {
        super.validateConnection(settings, cp);

        Config hiveConfig = settings.getConfig("hive-connection");
        hiveConfig.getBoolean("http-mode");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean loadValidatedConnection(final ConfigRO settings, final CredentialsProvider cp)
        throws InvalidSettingsException {
        boolean b = super.loadValidatedConnection(settings, cp);

        Config hiveConfig = settings.getConfig("hive-connection");
        m_httpMode = hiveConfig.getBoolean("http-mode");
        setRowIdsStartWithZero(true);

        return b;
    }
}
