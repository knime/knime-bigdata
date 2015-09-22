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
package com.knime.bigdata.spark.node.mllib.reduction.svd;

import java.util.LinkedList;
import java.util.List;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelDouble;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.node.mllib.MLlibNodeSettings;
import com.knime.bigdata.spark.node.mllib.MLlibSettings;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDs;

/**
 *
 * @author knime
 */
public class MLlibSVDNodeModel extends SparkNodeModel {

    private final SettingsModelIntegerBounded m_noOfSingularValues = createNoSingularValuesModel();

    private final SettingsModelBoolean m_computeU = createComputeUModel();

    private final SettingsModelDouble m_reciprocalCondition = createReciprocalConditionModel();

    private final MLlibNodeSettings m_settings = new MLlibNodeSettings(false);

    /**
     *
     */
    public MLlibSVDNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{BufferedDataTable.TYPE, SparkDataPortObject.TYPE, SparkDataPortObject.TYPE_OPTIONAL});
    }

    /**
     * @return
     */
    static SettingsModelDouble createReciprocalConditionModel() {
        return new SettingsModelDouble("reciprocalCondition", 1e-9);
    }

    /**
     * @return the compute U model
     */
    static SettingsModelBoolean createComputeUModel() {
        return new SettingsModelBoolean("computeU", false);
    }

    /**
     * @return
     */
    static SettingsModelIntegerBounded createNoSingularValuesModel() {
        return new SettingsModelIntegerBounded("noOfSingularValues", 10, 1, Integer.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        final DataTableSpec tableSpec = spec.getTableSpec();
        m_settings.check(tableSpec);
        final DataTableSpec svSpec = createSVSpec();
        final DataTableSpec vSpec = getVSpec(tableSpec);
        final DataTableSpec uSpec = getUSpec();
        return new PortObjectSpec[]{svSpec, new SparkDataPortObjectSpec(spec.getContext(), vSpec),
            uSpec == null ? null : new SparkDataPortObjectSpec(spec.getContext(), uSpec)};
    }

    /**
     * @param tableSpec
     * @return
     * @throws InvalidSettingsException
     */
    private DataTableSpec getVSpec(final DataTableSpec tableSpec) throws InvalidSettingsException {
        final int dim = m_noOfSingularValues.getIntValue();
        final DataTableSpec vSpec = createTableSpec(dim, "Dimension ");
        return vSpec;
    }

    /**
     * @return
     */
    private DataTableSpec getUSpec() {
        final int dim = m_noOfSingularValues.getIntValue();
        DataTableSpec uSpec = null;
        if (m_computeU.getBooleanValue()) {
            uSpec = createTableSpec(dim, "Dimension ");
        }
        return uSpec;
    }

    /**
     * @param colPrefix
     * @param context the {@link KNIMESparkContext} to use
     * @param i the number of principal components
     * @return the {@link SparkDataPortObjectSpec}
     */
    private static DataTableSpec createTableSpec(final int noOfCols, final String colPrefix) {
        final List<DataColumnSpec> specs = new LinkedList<>();
        final DataColumnSpecCreator specCreator = new DataColumnSpecCreator("Test", DoubleCell.TYPE);
        for (int i = 0; i < noOfCols; i++) {
            specCreator.setName(colPrefix + i);
            specs.add(specCreator.createSpec());
        }
        return new DataTableSpec(specs.toArray(new DataColumnSpec[0]));
    }

    /**
     * @return
     */
    private DataTableSpec createSVSpec() {
        DataColumnSpecCreator creator = new DataColumnSpecCreator("Singular Value", DoubleCell.TYPE);
        return new DataTableSpec(creator.createSpec());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[0];
        exec.setMessage("Starting SVD (SPARK)");
        exec.checkCanceled();
        final DataTableSpec tableSpec = data.getTableSpec();
        final MLlibSettings settings = m_settings.getSettings(tableSpec);
        int k = m_noOfSingularValues.getIntValue();
        boolean computeU = m_computeU.getBooleanValue();
        double recCond = m_reciprocalCondition.getDoubleValue();
        final DataTableSpec svSpec = createSVSpec();
        final DataTableSpec vSpec = getVSpec(tableSpec);
        final DataTableSpec uSpec = getUSpec();
        final String vMatrixName = SparkIDs.createRDDID();
        final String uMatrixName = SparkIDs.createRDDID();
        final SVDTask task =
                new SVDTask(data.getData(), settings.getFeatueColIdxs(), computeU, k, recCond, vMatrixName, uMatrixName);
        final double[] singularValues = task.execute(exec);
        final BufferedDataContainer dc = exec.createDataContainer(svSpec);
        for (int i = 0, length = singularValues.length; i < length; i++) {
            double d = singularValues[i];
            dc.addRowToTable(new DefaultRow(RowKey.createRowKey(i), new DoubleCell(d)));
        }
        dc.close();
        exec.setMessage("SVD (SPARK) done.");
        final SparkDataTable vMatrixRDD = new SparkDataTable(data.getContext(), vMatrixName, vSpec);
        SparkDataTable uMatixRDD = null;
        if (computeU) {
            uMatixRDD = new SparkDataTable(data.getContext(), uMatrixName, uSpec);
        }
        return new PortObject[]{dc.getTable(), new SparkDataPortObject(vMatrixRDD),
            uMatixRDD == null ? null : new SparkDataPortObject(uMatixRDD)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_noOfSingularValues.saveSettingsTo(settings);
        m_computeU.saveSettingsTo(settings);
        m_reciprocalCondition.saveSettingsTo(settings);
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noOfSingularValues.validateSettings(settings);
        m_computeU.validateSettings(settings);
        m_reciprocalCondition.validateSettings(settings);
        m_settings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noOfSingularValues.loadSettingsFrom(settings);
        m_computeU.loadSettingsFrom(settings);
        m_reciprocalCondition.loadSettingsFrom(settings);
        m_settings.loadSettingsFrom(settings);
    }
}
