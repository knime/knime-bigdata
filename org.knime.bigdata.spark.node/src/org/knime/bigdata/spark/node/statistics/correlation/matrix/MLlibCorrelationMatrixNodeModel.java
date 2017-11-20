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
 *   Created on 13.09.2015 by koetter
 */
package org.knime.bigdata.spark.node.statistics.correlation.matrix;

import java.util.List;

import org.knime.base.node.preproc.correlation.pmcc.PMCCPortObjectAndSpec;
import org.knime.base.util.HalfDoubleMatrix;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.job.HalfDoubleMatrixJobOutput;
import org.knime.bigdata.spark.core.job.JobRunFactory;
import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.node.statistics.correlation.CorrelationJobInput;
import org.knime.bigdata.spark.node.statistics.correlation.MLlibCorrelationMethod;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTableHolder;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibCorrelationMatrixNodeModel extends SparkNodeModel implements BufferedDataTableHolder {

    private final MLlibNodeSettings m_settings = new MLlibNodeSettings(false);

    private final SettingsModelString m_method = createMethodModel();

    private BufferedDataTable m_correlationTable;

    /** The unique Spark job id. */
    public static final String JOB_ID = MLlibCorrelationMatrixNodeModel.class.getCanonicalName();

    /**
     * Constructor.
     */
    MLlibCorrelationMatrixNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, BufferedDataTable.TYPE,
            PMCCPortObjectAndSpec.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelString createMethodModel() {
        return new SettingsModelString("correlationMethod", MLlibCorrelationMethod.getDefault().getActionCommand());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] ==  null) {
            return null;
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec) inSpecs[0];
        m_settings.check(spec.getTableSpec());
        final MLlibSettings settings = m_settings.getSettings(spec.getTableSpec());
        final String[] includes = settings.getFatureColNames().toArray(new String[0]);
        final DataTableSpec resultSpec = createSpec(settings);
        return new PortObjectSpec[]{
            new SparkDataPortObjectSpec(spec.getContextID(), resultSpec),
            PMCCPortObjectAndSpec.createOutSpec(includes), new PMCCPortObjectAndSpec(includes)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inData[0];
        final SparkContextID contextID = data.getContextID();
        final MLlibSettings settings = m_settings.getSettings(data.getTableSpec());

        final SparkDataTable resultTable =
            new SparkDataTable(data.getContextID(), createSpec(settings));

        final MLlibCorrelationMethod method = MLlibCorrelationMethod.get(m_method.getStringValue());
        final CorrelationJobInput input =
                new CorrelationJobInput(data.getTableName(), method.getMethod(), resultTable.getID(), settings.getFeatueColIdxs());
        final JobRunFactory<CorrelationJobInput, HalfDoubleMatrixJobOutput> runFactory = getJobRunFactory(data, JOB_ID);
        final HalfDoubleMatrixJobOutput output = runFactory.createRun(input).run(contextID, exec);
        final HalfDoubleMatrix correlationMatrix = new HalfDoubleMatrix(output.getMatrix(), output.storesDiagonal());
        final String[] includeNames = settings.getFatureColNames().toArray(new String[0]);
        final PMCCPortObjectAndSpec pmccModel =
                new PMCCPortObjectAndSpec(includeNames, correlationMatrix);
        final BufferedDataTable out = pmccModel.createCorrelationMatrix(exec);
        m_correlationTable = out;
        return new PortObject[]{new SparkDataPortObject(resultTable), out, pmccModel};
    }

    /** {@inheritDoc} */
    @Override
    public BufferedDataTable[] getInternalTables() {
        return new BufferedDataTable[]{m_correlationTable};
    }

    /** {@inheritDoc} */
    @Override
    public void setInternalTables(final BufferedDataTable[] tables) {
        m_correlationTable = tables[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void resetInternal() {
        m_correlationTable = null;
    }

    /**
     * @param settings
     * @return
     */
    private DataTableSpec createSpec(final MLlibSettings settings) {
        final List<String> featureColNames = settings.getFatureColNames();
        final DataColumnSpecCreator creator = new DataColumnSpecCreator("Dummy", DoubleCell.TYPE);
        final DataColumnSpec specs[] = new DataColumnSpec[featureColNames.size()];
        int idx = 0;
        for (final String colName : featureColNames) {
            creator.setName(colName);
            specs[idx++] = creator.createSpec();
        }
        return new DataTableSpec(specs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
        m_method.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
        m_method.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
        m_method.loadSettingsFrom(settings);
    }
}
