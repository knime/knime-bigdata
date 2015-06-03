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

import java.util.Collection;
import java.util.LinkedList;

import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnFilter2;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;

import com.knime.bigdata.spark.node.AbstractSparkNodeModel;
import com.knime.bigdata.spark.node.mllib.clustering.assigner.MLlibClusterAssignerNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.port.model.SparkModelPortObjectSpec;
import com.knime.bigdata.spark.util.SparkIDGenerator;


/**
 *
 * @author knime
 */
public class MLlibDecisionTreeNodeModel extends AbstractSparkNodeModel {

    private final SettingsModelString m_classCol = createClassColModel();

    private final SettingsModelColumnFilter2 m_cols = createColumnsModel();

    //    private final SettingsModelIntegerBounded m_noOfCluster = createNoOfClusterModel();
//    private final SettingsModelIntegerBounded m_noOfIteration = createNoOfIterationModel();
//    private final SettingsModelString m_tableName = createTableNameModel();
//    private final SettingsModelString m_colName = createColumnNameModel();

    /**
     *
     */
    public MLlibDecisionTreeNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, SparkModelPortObject.TYPE});
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
        if (inSpecs == null || inSpecs.length != 1) {
            throw new InvalidSettingsException("No Hive connection available");
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        DataTableSpec tableSpec = spec.getTableSpec();
        FilterResult result = m_cols.applyTo(tableSpec);
        final String[] includedCols = result.getIncludes();
        if (includedCols == null || includedCols.length < 1) {
            throw new InvalidSettingsException("No input columns available");
        }
        return new PortObjectSpec[]{MLlibClusterAssignerNodeModel.createSpec(tableSpec), createMLSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[0];
        final String aOutputTableName = SparkIDGenerator.createID();
        exec.setMessage("Starting Decision Tree (SPARK) Learner");
        exec.checkCanceled();
        final DataTableSpec tableSpec = data.getTableSpec();
        final DataTableSpec resultSpec = MLlibClusterAssignerNodeModel.createSpec(tableSpec);
        SparkDataTable resultRDD = new SparkDataTable(data.getContext(), aOutputTableName, resultSpec);

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
        final DecisionTreeTask task = new DecisionTreeTask(data.getData(), numericColIdx, classColName, classColIdx, resultRDD);
        final DecisionTreeModel model = task.execute(exec);
        return new PortObject[] {new SparkModelPortObject<>(new SparkModel<>("DecisionTree", model))};

    }

    /**
     * @return
     */
    private SparkModelPortObjectSpec createMLSpec() {
        return new SparkModelPortObjectSpec("DecisionTree");
    }

    /**
     * @return
     */
    @SuppressWarnings("unchecked")
    static SettingsModelColumnFilter2 createColumnsModel() {
        return new SettingsModelColumnFilter2("columns", DoubleValue.class);
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

}
