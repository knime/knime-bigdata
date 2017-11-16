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
 *   Created on 26.06.2015 by koetter
 */
package com.knime.bigdata.spark.node.io.table.writer;

import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.streamable.BufferedDataTableRowOutput;
import org.knime.core.node.streamable.OutputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.StreamableOperator;

import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;
import com.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import com.knime.bigdata.spark.node.SparkNodePlugin;

/**
 * Node model that downloads a {@link SparkDataTable} and converts it into a KNIME {@link DataTable}.
 * <p>/
 * The output port of this node is streamable. Also, this node behaves differently when executed in KNIME-on-Spark mode.
 *
 * @author Tobias Koetter, KNIME GmbH
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class Spark2TableNodeModel extends SparkNodeModel {

    private final SettingsModelString m_knospInputID = new SettingsModelString("knospInputID", null);

    private final SettingsModelIntegerBounded m_fetchSize = createFetchSizeModel();

    private final SettingsModelBoolean m_fetchAll = createFetchAllModel();

    /**Constructor.*/
    Spark2TableNodeModel() {
        super(new PortType[] {SparkDataPortObject.TYPE}, new PortType[] {BufferedDataTable.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelBoolean createFetchAllModel() {
        return new SettingsModelBoolean("fetchAll", false);
    }

    /**
     * @return fetch size model
     */
    static SettingsModelIntegerBounded createFetchSizeModel() {
        final SettingsModelIntegerBounded model =
                new SettingsModelIntegerBounded("fetchSize", 1000, 0, Integer.MAX_VALUE);
        return model;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("Please connect the input port");
        }
        if (!m_fetchAll.getBooleanValue() && !inKNOSPMode()) {
            //warn the user that we only retrieve the top k rows
            setWarningMessage("Fetching only first " + m_fetchSize.getIntValue() + " rows");
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        return new PortObjectSpec[] { SparkDataTableUtil.getTransferredSparkDataTableSpec(spec.getTableSpec()) };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        if (inData == null || inData.length != 1 || inData[0] == null) {
            throw new InvalidSettingsException("Please connect the input port");
        }

        final SparkDataPortObject sparkData = (SparkDataPortObject)inData[0];
        final DataTableSpec sparkDataSpec = sparkData.getTableSpec();
        final BufferedDataTableRowOutput rowOutput = new BufferedDataTableRowOutput(
            exec.createDataContainer(SparkDataTableUtil.getTransferredSparkDataTableSpec(sparkDataSpec)));

        // this fetches the rows and pushes them into rowOutput
        createStreamableOperatorInternal().runWithRowOutput(sparkData.getData(), rowOutput, exec);

        exec.setMessage("Creating a buffered data table...");
        return new PortObject[]{rowOutput.getDataTable()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_fetchSize.saveSettingsTo(settings);
        m_fetchAll.saveSettingsTo(settings);
        m_knospInputID.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fetchSize.validateSettings(settings);
        m_fetchAll.validateSettings(settings);
        if (settings.containsKey(m_knospInputID.getKey())) {
            m_knospInputID.validateSettings(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fetchSize.loadSettingsFrom(settings);
        m_fetchAll.loadSettingsFrom(settings);
        if (settings.containsKey(m_knospInputID.getKey())) {
            m_knospInputID.loadSettingsFrom(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo,
        final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        return createStreamableOperatorInternal();
    }

    private AbstractSpark2TableStreamableOperator createStreamableOperatorInternal() {
        if (inKNOSPMode()) {
            return SparkNodePlugin.getKNOSPHelper().createSpark2TableStreamableOperator(m_knospInputID.getStringValue());
        } else if (m_fetchAll.getBooleanValue()) {
            return new Spark2TableStreamableOperator();
        } else {
            return new Spark2TableStreamableOperator(m_fetchSize.getIntValue());
        }

    }

    private boolean inKNOSPMode() {
        return m_knospInputID.getStringValue() != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputPortRole[] getOutputPortRoles() {
        return new OutputPortRole[]{OutputPortRole.DISTRIBUTED};
    }

    /**
     * Puts this node model into KNIME-on-Spark (KNOSP) mode. In KNOSP mode, the node model will read rows from a
     * {@link StreamableOperator} that is sourced from a single partition of the input RDD/Dataset.
     *
     * @param knospInputID Key required to obtain the {@link StreamableOperator}.
     */
    public void activateKNOSPMode(final String knospInputID) {
        m_knospInputID.setStringValue(knospInputID);
    }
}
