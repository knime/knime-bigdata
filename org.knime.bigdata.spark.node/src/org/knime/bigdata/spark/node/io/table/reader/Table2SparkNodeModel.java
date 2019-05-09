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
package org.knime.bigdata.spark.node.io.table.reader;

import java.io.File;
import java.io.IOException;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.node.SparkNodePlugin;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.StreamableOperator;

/**
 * Node model that uploads a a KNIME {@link DataTable} into a Spark cluster and provides a {@link SparkDataTable} that
 * references the (now remote) data.
 * <p>/
 * The input port of this node is streamable. Also, this node behaves differently when executed in KNIME-on-Spark mode.
 *
 * @author Tobias Koetter, KNIME.com
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class Table2SparkNodeModel extends SparkSourceNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Table2SparkNodeModel.class);

    /**The unique Spark job id.*/
    public static final String JOB_ID = "Table2SparkJob";

    private final SettingsModelString m_knospOutputID = new SettingsModelString("knospOutputID", null);

    private final boolean m_isDeprecatedNode;

    /**
     * Default constructor.
     *
     * @param isDeprecatedNode Whether this node model instance should emulate the behavior of the deprecated
     *            table2spark node model.
     */
    public Table2SparkNodeModel(final boolean isDeprecatedNode) {
        super(new PortType[]{BufferedDataTable.TYPE}, isDeprecatedNode, new PortType[]{SparkDataPortObject.TYPE});
        m_isDeprecatedNode = isDeprecatedNode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("Please connect the input port");
        }

        // if you change the spec behavior, you also need to change the behavior in the streamable operator implementation(s)
        // and in executeInternal()
        final DataTableSpec spec = (DataTableSpec)inSpecs[0];
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(getContextID(inSpecs), createSparkDataTableSpec(spec))};
    }

	private boolean isKNOSPMode() {
        return m_knospOutputID.getStringValue() != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        if (inData == null || inData.length < 1 || inData[0] == null) {
            throw new InvalidSettingsException("Please connect the input port");
        }
        //Check that the context is available before doing all the work
        final SparkContextID contextID = getContextID(inData);
        final BufferedDataTable inputTable = (BufferedDataTable)inData[0];

        final AbstractTable2SparkStreamableOperator streamableOp = createStreamableOperatorInternal(contextID);
        streamableOp.runWithRowInput(new DataTableRowInput(inputTable), exec);

        // if you change the spec behavior, you also need to change the behavior in the streamable operator implementation and in configureInternal()
        return new PortObject[]{new SparkDataPortObject(
            streamableOp.createSparkDataTable(contextID, createSparkDataTableSpec(inputTable.getDataTableSpec())))};
    }


    /**
     * Utility method to create the spec of the {@link SparkDataTable} which is passed to the next node. Currently, this
     * method produces a spec that has identical column names and types to the given one, but it drops any additional
     * table or column metadata (value domains, color handler, ...) because they don't make sense if you don't have the
     * data locally.
     *
     * @param knimeTableSpec The spec of the {@link DataTable} that is uploaded to Spark.
     * @return the spec of the {@link SparkDataTable}
     */
    public static DataTableSpec createSparkDataTableSpec(final DataTableSpec knimeTableSpec) {
        final DataColumnSpec[] sparkDataColumns = new DataColumnSpec[knimeTableSpec.getNumColumns()];
        for (int i = 0; i < sparkDataColumns.length; i++) {
            final DataColumnSpec knimeColumnSpec = knimeTableSpec.getColumnSpec(i);
            sparkDataColumns[i] = new DataColumnSpecCreator(knimeColumnSpec.getName(), knimeColumnSpec.getType()).createSpec();
        }
        return new DataTableSpec(sparkDataColumns);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
    	m_knospOutputID.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		if (settings.containsKey(m_knospOutputID.getKey())) {
            m_knospOutputID.validateSettings(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		if (settings.containsKey(m_knospOutputID.getKey())) {
            m_knospOutputID.loadSettingsFrom(settings);
        }
    }

    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo,
        final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        return createStreamableOperatorInternal(getContextID(inSpecs));
    }

    private AbstractTable2SparkStreamableOperator createStreamableOperatorInternal(final SparkContextID contextID) {
        if (isKNOSPMode()) {
            // KNIME-on-Spark mode (running inside a JVM of a Spark executor in a cluster)
            return SparkNodePlugin.getKNOSPHelper().createTable2SparkStreamableOperator(m_knospOutputID.getStringValue());
        } else {
            final File tmpFile = createTempFile();
            // the deprecated table table2spark node did create a Spark context when necessary, the
            // current does not anymore, hence ensureSparkContext = m_isDeprecatedNode
            return new Table2SparkStreamableOperator(contextID, tmpFile, m_isDeprecatedNode);
        }
    }

    private File createTempFile() {
        // first we need to create a temp file that is deleted after executing this node
        final File tmpFile;
        try {
            tmpFile = File.createTempFile("knime-table2spark", ".tmp");
            LOGGER.debugWithFormat("Serializing data table to file %s", tmpFile.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp file: " + e.getMessage(), e);
        }
        addFileToDeleteAfterExecute(tmpFile);
        return tmpFile;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputPortRole[] getInputPortRoles() {
        return new InputPortRole[]{InputPortRole.NONDISTRIBUTED_STREAMABLE, InputPortRole.NONDISTRIBUTED_NONSTREAMABLE};
    }

    /**
     * Puts this node model into KNIME-on-Spark (KNOSP) mode. In KNOSP mode, the ingoing KNIME data table will
     * not be uploaded to Spark, but written out to an in-memory data structure obtained using the provided key.
     *
     * @param knospOutputID a unique key under which an output queue will be looked up
     */
    public void activateKNOSPMode(final String knospOutputID) {
        m_knospOutputID.setStringValue(knospOutputID);
    }
}
