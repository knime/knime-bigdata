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

import org.apache.spark.mllib.clustering.KMeansModel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnFilter2;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
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
import com.knime.bigdata.spark.util.SparkUtil;

/**
 *
 * @author knime
 */
public class MLlibKMeansNodeModel extends AbstractSparkNodeModel {

    private final SettingsModelIntegerBounded m_noOfCluster = createNoOfClusterModel();

    private final SettingsModelIntegerBounded m_noOfIteration = createNoOfIterationModel();

    private final SettingsModelColumnFilter2 m_cols = createColumnsModel();

    /**
     *
     */
    public MLlibKMeansNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, SparkModelPortObject.TYPE});
    }

    /**
     * @return
     */
    @SuppressWarnings("unchecked")
    static SettingsModelColumnFilter2 createColumnsModel() {
        return new SettingsModelColumnFilter2("columns", DoubleValue.class);
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
        exec.setMessage("Starting KMeans (SPARK) Learner");
        exec.checkCanceled();
        final DataTableSpec tableSpec = data.getTableSpec();
        final FilterResult result = m_cols.applyTo(tableSpec);
        final String[] includedCols = result.getIncludes();
        final int[] includeColIdxs = SparkUtil.getColumnIndices(tableSpec, includedCols);
        final DataTableSpec resultSpec = MLlibClusterAssignerNodeModel.createSpec(tableSpec);
        final String aOutputTableName = SparkIDGenerator.createID();
        final SparkDataTable resultRDD = new SparkDataTable(data.getContext(), aOutputTableName, resultSpec);
        final KMeansTask task = new KMeansTask(data.getData(), includeColIdxs, m_noOfCluster.getIntValue(),
            m_noOfIteration.getIntValue(), resultRDD);
        final KMeansModel clusters = task.execute(exec);
        exec.setMessage("KMeans (SPARK) Learner done.");
        return new PortObject[]{new SparkDataPortObject(resultRDD), new SparkModelPortObject<>(new SparkModel<>(
                "KMeans", clusters, MLlibKMeansInterpreter.getInstance(), tableSpec, null, includedCols))};
    }


    /**
     * @return
     */
    private SparkModelPortObjectSpec createMLSpec() {
        return new SparkModelPortObjectSpec("kmeans");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_noOfCluster.saveSettingsTo(settings);
        m_noOfIteration.saveSettingsTo(settings);
        m_cols.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noOfCluster.validateSettings(settings);
        m_noOfIteration.validateSettings(settings);
        m_cols.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noOfCluster.loadSettingsFrom(settings);
        m_noOfIteration.loadSettingsFrom(settings);
        m_cols.loadSettingsFrom(settings);
    }
}
