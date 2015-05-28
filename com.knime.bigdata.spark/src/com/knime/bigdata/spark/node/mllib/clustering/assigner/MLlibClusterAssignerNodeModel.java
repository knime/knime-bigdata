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
 *   Created on 12.02.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.clustering.assigner;

import java.io.File;
import java.io.IOException;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.port.JavaRDDPortObject;
import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.SparkModelPortObject;

/**
 *
 * @author koetter
 */
public class MLlibClusterAssignerNodeModel extends NodeModel {

    private final SettingsModelString m_hiveQuery = createHiveStatementModel();

    private final SettingsModelString m_colName = createColumnNameModel();

    /**
     *
     */
    public MLlibClusterAssignerNodeModel() {
        super(new PortType[]{SparkModelPortObject.TYPE}, new PortType[]{JavaRDDPortObject.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelString createHiveStatementModel() {
        return new SettingsModelString("hiveQuery", "select * from ");
    }

    /**
     * @return
     */
    static SettingsModelString createColumnNameModel() {
        return new SettingsModelString("columnName", "Cluster");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new PortObjectSpec[]{createSpec(null)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final String aOutputTableName = "kmeansPrediction" + System.currentTimeMillis();
        @SuppressWarnings("unchecked")
        final SparkModel<KMeansModel> model = ((SparkModelPortObject<KMeansModel>)inObjects[0]).getModel();
        exec.checkCanceled();
        exec.setMessage("Starting KMeans (SPARK) Predictor");

        final String contextName = KnimeContext.getSparkContext();
        final AssignTask task = new AssignTask();
        task.execute(contextName, exec, m_hiveQuery.getStringValue(), model.getModel(), aOutputTableName);

        exec.setMessage("KMeans (SPARK) Prediction done.");
        return new PortObject[]{};
    }

    /**
     */
    public static DataTableSpec createSpec(final DataTableSpec originalSpec) throws InvalidSettingsException {
        final String clusterColName = DataTableSpec.getUniqueColumnName(originalSpec, "Cluster");
        DataColumnSpecCreator creator = new DataColumnSpecCreator(clusterColName, StringCell.TYPE);
        DataColumnSpec labelColSpec = creator.createSpec();
        return new DataTableSpec(originalSpec, new DataTableSpec(labelColSpec));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_hiveQuery.saveSettingsTo(settings);
        m_colName.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_hiveQuery.validateSettings(settings);
        m_colName.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_hiveQuery.loadSettingsFrom(settings);
        m_colName.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
        CanceledExecutionException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
        CanceledExecutionException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // nothing to do
    }
}
