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
 *   Created on Feb 12, 2015 by knime
 */
package com.knime.bigdata.spark.node.mllib.clustering.kmeans;

import java.io.File;
import java.io.IOException;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabasePortObjectSpec;

import com.knime.bigdata.spark.node.mllib.clustering.assigner.MLlibClusterAssignerNodeModel;
import com.knime.bigdata.spark.port.JavaRDDPortObject;
import com.knime.bigdata.spark.port.MLlibModel;
import com.knime.bigdata.spark.port.MLlibPortObject;
import com.knime.bigdata.spark.port.MLlibPortObjectSpec;

/**
 *
 * @author knime
 */
public class MLlibKMeansNodeModel extends NodeModel {

    private final SettingsModelIntegerBounded m_noOfCluster = createNoOfClusterModel();

    private final SettingsModelIntegerBounded m_noOfIteration = createNoOfIterationModel();

    private final SettingsModelString m_hiveQuery = createHiveQueryModel();

//    private final SettingsModelString m_tableName = createTableNameModel();
//
//    private final SettingsModelString m_colName = createColumnNameModel();

    /**
     *
     */
    public MLlibKMeansNodeModel() {
        //        super(new PortType[]{DatabasePortObject.TYPE},
        //            new PortType[]{DatabasePortObject.TYPE, MLlibPortObject.TYPE});
        super(new PortType[]{}, new PortType[]{JavaRDDPortObject.TYPE, MLlibPortObject.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelString createTableNameModel() {
        return new SettingsModelString("tableName", "input");
    }

    /**
     * @return
     */
    static SettingsModelString createHiveQueryModel() {
        return new SettingsModelString("hiveQuery", "select * from iris");
    }

    /**
     * @return
     */
    static SettingsModelString createColumnNameModel() {
        return new SettingsModelString("columnName", "Cluster");
    }

    /**
     * @return
     */
    static SettingsModelIntegerBounded createNoOfClusterModel() {
        return new SettingsModelIntegerBounded("noOfCluster", 3, 1, Integer.MAX_VALUE);
    }

    /**
     * @return
     */
    static SettingsModelIntegerBounded createNoOfIterationModel() {
        return new SettingsModelIntegerBounded("noOfIteration", 30, 1, Integer.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        //        final DatabasePortObjectSpec spec = (DatabasePortObjectSpec) inSpecs[0];
        //        if (!spec.getDatabaseIdentifier().equals(DATABASE_IDENTIFIER)) {
        //            throw new InvalidSettingsException("Only Hive connections are supported");
        //        }
        return new PortObjectSpec[]{MLlibClusterAssignerNodeModel.createSpec(m_hiveQuery.getStringValue()),createMLSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final String aOutputTableName = "kmeansTrainPrediction" + System.currentTimeMillis();
        exec.setMessage("Starting KMeans (SPARK) Learner");
        exec.checkCanceled();
        final KMeansTask task =
            new KMeansTask(m_hiveQuery.getStringValue(), m_noOfCluster.getIntValue(),
                m_noOfIteration.getIntValue(), aOutputTableName);
        final KMeansModel clusters = task.execute(exec);
        exec.setMessage("KMeans (SPARK) Learner done.");

        DatabasePortObjectSpec dbSpec = MLlibClusterAssignerNodeModel.createSpec(aOutputTableName);
        return new PortObject[]{
            new JavaRDDPortObject(dbSpec),
            new MLlibPortObject<>(new MLlibModel<>("KMeans", clusters))};
    }


    /**
     * @return
     */
    private MLlibPortObjectSpec createMLSpec() {
        return new MLlibPortObjectSpec("kmeans");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_noOfCluster.saveSettingsTo(settings);
        m_noOfIteration.saveSettingsTo(settings);
        //m_tableName.saveSettingsTo(settings);
        m_hiveQuery.saveSettingsTo(settings);
        //m_colName.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noOfCluster.validateSettings(settings);
        m_noOfIteration.validateSettings(settings);
        //m_tableName.validateSettings(settings);
        m_hiveQuery.validateSettings(settings);
        //m_colName.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noOfCluster.loadSettingsFrom(settings);
        m_noOfIteration.loadSettingsFrom(settings);
        //m_tableName.loadSettingsFrom(settings);
        m_hiveQuery.loadSettingsFrom(settings);
        //m_colName.loadSettingsFrom(settings);
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
