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
 *   Created on Sep 5, 2017 by bjoern
 */
package com.knime.bigdata.spark.core.node;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.port.PortObjectSpec;
import org.osgi.framework.Version;

import com.knime.bigdata.spark.core.version.SparkPluginVersion;

/**
 * Base implementation of {@link DefaultNodeSettingsPane} to be used by Spark nodes instead of the standard
 * {@łink DefaultNodeSettingsPane}.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @since 2.1.0
 */
public class SparkDefaultNodeSettingsPane extends DefaultNodeSettingsPane {

    /**
     * The OSGI version of KNIME Spark Executor (technically, of com.knime.bigdata.spark.core) with which the node was
     * added to the KNIME workflow. If defined, this value must remain unchanged by the node dialog.
     */
    private Version m_knimeSparkExecutorVersion;

    /**
     * Constructor.
     */
    protected SparkDefaultNodeSettingsPane() {
        m_knimeSparkExecutorVersion = SparkPluginVersion.VERSION_CURRENT;
    }

    /**
     * Provides the version of KNIME Spark Executor (technically, of com.knime.bigdata.spark.core) with which the node
     * was added to the KNIME workflow.
     *
     * @return the version as an OSGI {@link Version}.
     */
    public Version getKNIMESparkExecutorVersion() {
        return m_knimeSparkExecutorVersion;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void saveAdditionalSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        SparkNodeModel.saveKNIMESparkExecutorVersionTo(settings, m_knimeSparkExecutorVersion);
        saveAdditionalSparkSettingsTo(settings);
    }

    /**
     * This method can be overridden to save additional Spark node settings to the given settings object.
     *
     * @param settings the <code>NodeSettings</code> to write into
     * @throws InvalidSettingsException if the user has entered wrong values
     */
    protected void saveAdditionalSparkSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        // do nothing by default
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void loadAdditionalSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        try {
            m_knimeSparkExecutorVersion = SparkNodeModel.loadKNIMESparkExecutorVersionFrom(settings);
        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage(), e);
        }

        super.loadAdditionalSettingsFrom(settings, specs);
        loadAdditionalSparkSettingsFrom(settings, specs);
    }

    /**
     * This method can be overridden to load additional Spark node settings. This method is called by
     * {@link #loadAdditionalSettingsFrom(NodeSettingsRO, PortObjectSpec[])}.
     * <p>
     * See {@link #loadAdditionalSettingsFrom(NodeSettingsRO, PortObjectSpec[])} for further documentation.
     *
     * @param settings The settings to read.
     * @param specs The input data table specs. Items of the array could be null if no spec is available from the
     *            corresponding input port (i.e. not connected or upstream node does not produce an output spec). If a
     *            port is of type {@link BufferedDataTable#TYPE} and no spec is available the framework will replace
     *            null by an empty {@link DataTableSpec} (no columns) unless the port is marked as optional.
     *
     * @throws NotConfigurableException if the dialog cannot be opened because of real invalid settings or if any
     *             preconditions are not fulfilled, e.g. no predecessor node, no nominal column in input table, etc.
     *
     * @see #loadSettingsFrom(NodeSettingsRO, PortObjectSpec[])
     */
    protected void loadAdditionalSparkSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        // do nothing by default
    }
}
