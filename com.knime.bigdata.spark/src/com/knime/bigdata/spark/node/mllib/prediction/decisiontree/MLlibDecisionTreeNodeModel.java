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
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
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
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.workflow.CredentialsProvider;

import com.knime.bigdata.hive.utility.HiveUtility;
import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.port.model.SparkModelPortObjectSpec;


/**
 *
 * @author knime
 */
public class MLlibDecisionTreeNodeModel extends NodeModel {

    private static final String DATABASE_IDENTIFIER = HiveUtility.DATABASE_IDENTIFIER;
    private final SettingsModelString m_classCol = createClassColModel();
//    private final SettingsModelIntegerBounded m_noOfCluster = createNoOfClusterModel();
//    private final SettingsModelIntegerBounded m_noOfIteration = createNoOfIterationModel();
//    private final SettingsModelString m_tableName = createTableNameModel();
//    private final SettingsModelString m_colName = createColumnNameModel();

    /**
     *
     */
    public MLlibDecisionTreeNodeModel() {
        super(new PortType[]{DatabasePortObject.TYPE},
            new PortType[]{SparkModelPortObject.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelString createClassColModel() {
        return new SettingsModelString("classCol", null);
    }

    /**
     * @return
     */
    static SettingsModelString createTableNameModel() {
        return new SettingsModelString("tableName", "result");
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
        DatabasePortObjectSpec spec = (DatabasePortObjectSpec) inSpecs[0];
        if (!spec.getDatabaseIdentifier().equals(DATABASE_IDENTIFIER)) {
            throw new InvalidSettingsException("Only Hive connections are supported");
        }
        return new PortObjectSpec[] {createMLSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final DatabasePortObject db = (DatabasePortObject)inObjects[0];
        final DataTableSpec tableSpec = db.getSpec().getDataTableSpec();
//        final String resultTableName = m_tableName.getStringValue();
        final CredentialsProvider cp = getCredentialsProvider();
        final DatabaseQueryConnectionSettings connSettings = db.getConnectionSettings(cp);
        exec.setMessage("Connecting to Spark...");
        final SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("knimeTest");
//        conf.set("hive.metastore.uris", "thrift://sandbox.hortonworks.com:9083");
        try (final JavaSparkContext sc = new JavaSparkContext(conf);) {
        exec.checkCanceled();
        exec.setMessage("Connecting to Hive...");
        final JavaHiveContext sqlsc = new JavaHiveContext(sc);
        exec.setMessage("Execute Hive Query...");
        final Collection<Integer> numericColIdx = new LinkedList<>();
        int classColIdx = -1;
        int counter = 0;
        final String classColName = m_classCol.getStringValue();
        for (DataColumnSpec colSpec : tableSpec) {
            if (colSpec.getName().equals(classColName)) {
                classColIdx = counter;
            }
            if (colSpec.getType().isCompatible(DoubleValue.class)) {
                numericColIdx.add(Integer.valueOf(counter));
            }
            counter++;
        }
        final String sql = connSettings.getQuery();
        final DecisionTreeTask task = new DecisionTreeTask(sql, numericColIdx, classColName, classColIdx);
        final DecisionTreeModel model = task.execute(sqlsc);
//        KMeansModel clusters = new KMeansModel(new Vector[] {new DenseVector(new double[] {1,0,1})});
        return new PortObject[] {new SparkModelPortObject<>(new SparkModel<>("DecisionTree", model))};
        }
    }

    /**
     * @param type
     * @return
     */
    private DataType getSparkType(final org.knime.core.data.DataType type) {
        if (type.isCompatible(DoubleValue.class)) {
            return DataType.DoubleType;
        }
        return DataType.StringType;
    }

    /**
     * @return
     */
    private SparkModelPortObjectSpec createMLSpec() {
        return new SparkModelPortObjectSpec("DecisionTree");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_classCol.saveSettingsTo(settings);
//        m_noOfCluster.saveSettingsTo(settings);
//        m_noOfIteration.saveSettingsTo(settings);
//        m_tableName.saveSettingsTo(settings);
//        m_colName.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_classCol.validateSettings(settings);
//        m_noOfCluster.validateSettings(settings);
//        m_noOfIteration.validateSettings(settings);
//        m_tableName.validateSettings(settings);
//        m_colName.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_classCol.loadSettingsFrom(settings);
//        m_noOfCluster.loadSettingsFrom(settings);
//        m_noOfIteration.loadSettingsFrom(settings);
//        m_tableName.loadSettingsFrom(settings);
//        m_colName.loadSettingsFrom(settings);
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
