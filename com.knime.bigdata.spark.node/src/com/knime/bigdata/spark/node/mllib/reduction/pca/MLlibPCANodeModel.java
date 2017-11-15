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
 *   Created on Feb 12, 2015 by knime
 */
package com.knime.bigdata.spark.node.mllib.reduction.pca;

import java.util.LinkedList;
import java.util.List;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.job.util.MLlibSettings;
import com.knime.bigdata.spark.core.node.MLlibNodeSettings;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.context.SparkContextConfig;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.core.util.SparkIDs;

/**
 *
 * @author knime
 */
public class MLlibPCANodeModel extends SparkNodeModel {

    private final SettingsModelIntegerBounded m_noOfComponents = createNoComponentsModel();

    private final MLlibNodeSettings m_settings = new MLlibNodeSettings(false);

    /**The unique job id.*/
    public static final String JOB_ID = MLlibPCANodeModel.class.getCanonicalName();

    /**
     *
     */
    public MLlibPCANodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, SparkDataPortObject.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelIntegerBounded createNoComponentsModel() {
        return new SettingsModelIntegerBounded("noOfPrincipalComponents", 10, 1, Integer.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        final DataTableSpec tableSpec = spec.getTableSpec();
        m_settings.check(tableSpec);
        final DataTableSpec projectedSpec = createResultSpec(m_noOfComponents.getIntValue(), "PCA dimension ");
        final DataTableSpec matrixSpec = createResultSpec(m_noOfComponents.getIntValue(), "Component ");
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(spec.getContextID(), projectedSpec),
            new SparkDataPortObjectSpec(spec.getContextID(), matrixSpec)};
    }

    /**
     * @param colPrefix
     * @param context the {@link SparkContextConfig} to use
     * @param i the number of principal components
     * @return the {@link SparkDataPortObjectSpec}
     */
    private static DataTableSpec createResultSpec(final int noOfComponents, final String colPrefix) {
        final List<DataColumnSpec> specs = new LinkedList<>();
        final DataColumnSpecCreator specCreator = new DataColumnSpecCreator("Test", DoubleCell.TYPE);
        for (int i = 0; i < noOfComponents; i++) {
            specCreator.setName(colPrefix + i);
            specs.add(specCreator.createSpec());
        }
        return new DataTableSpec(specs.toArray(new DataColumnSpec[0]));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[0];
        exec.setMessage("Starting PCA (SPARK)");
        exec.checkCanceled();
        final DataTableSpec tableSpec = data.getTableSpec();
        final MLlibSettings settings = m_settings.getSettings(tableSpec);
        int noOfComponents = m_noOfComponents.getIntValue();
        final DataTableSpec projectedSpec = createResultSpec(m_noOfComponents.getIntValue(), "DIM_");
        final DataTableSpec matrixSpec = createResultSpec(m_noOfComponents.getIntValue(), "Component_");
        final String matrixName = SparkIDs.createSparkDataObjectID();
        final String projectionMatrixName = SparkIDs.createSparkDataObjectID();
        final PCAJobInput jobInput = new PCAJobInput(data.getTableName(), settings.getFeatueColIdxs(), noOfComponents,
            matrixName, projectionMatrixName);
        SparkContextUtil.getSimpleRunFactory(data.getContextID(), JOB_ID).createRun(jobInput).run(data.getContextID());
        exec.setMessage("PCA (SPARK) done.");
        return new PortObject[]{createSparkPortObject(data, projectedSpec, projectionMatrixName),
            createSparkPortObject(data, matrixSpec, matrixName)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_noOfComponents.saveSettingsTo(settings);
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noOfComponents.validateSettings(settings);
        m_settings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noOfComponents.loadSettingsFrom(settings);
        m_settings.loadSettingsFrom(settings);
    }
}
