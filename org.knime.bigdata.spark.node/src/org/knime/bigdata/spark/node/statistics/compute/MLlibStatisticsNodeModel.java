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
 *   Created on 26.09.2015 by koetter
 */
package org.knime.bigdata.spark.node.statistics.compute;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.job.ColumnsJobInput;
import org.knime.bigdata.spark.core.job.JobRunFactory;
import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibStatisticsNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = MLlibStatisticsNodeModel.class.getCanonicalName();

    private final MLlibNodeSettings m_settings = new MLlibNodeSettings(false);

    /**
     * Constructor.
     */
    protected MLlibStatisticsNodeModel() {
        super(new PortType[] {SparkDataPortObject.TYPE}, new PortType[] {BufferedDataTable.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input data available");
        }
        m_settings.check(((SparkDataPortObjectSpec)inSpecs[0]).getTableSpec());
        return new PortObjectSpec[] {StatsRowCreator.createSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        if (inData == null || inData.length < 1 || inData[0] == null) {
            throw new InvalidSettingsException("No input data available");
        }
        exec.setMessage("Starting Spark statistics job...");
        final SparkDataPortObject data = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = data.getContextID();
        JobRunFactory<ColumnsJobInput, StatisticsJobOutput> runFactory =
                SparkContextUtil.getJobRunFactory(data.getContextID(), JOB_ID);
        final MLlibSettings settings = m_settings.getSettings(data);
        final ColumnsJobInput input = new ColumnsJobInput(data.getTableName(), settings.getFeatueColIdxs());
        final StatisticsJobOutput output = runFactory.createRun(input).run(contextID, exec);
        exec.setMessage("Spark statistics job finished.");
        exec.setMessage("Writing statistics table...");
        final StatsRowCreator rowCreator = new StatsRowCreator(output, settings);
        final BufferedDataTable table = rowCreator.createTable(exec);
        return new PortObject[] {table};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
    }

}
