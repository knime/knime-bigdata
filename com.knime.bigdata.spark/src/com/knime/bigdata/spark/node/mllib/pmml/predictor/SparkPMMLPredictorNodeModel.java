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
package com.knime.bigdata.spark.node.mllib.pmml.predictor;

import org.knime.base.node.mine.util.PredictorHelper;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDs;
import com.knime.bigdata.spark.util.SparkPMMLUtil;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObject;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObjectSpec;

/**
 *
 * @author koetter
 */
public class SparkPMMLPredictorNodeModel extends SparkNodeModel {

    private static final String CFG_KEY_OUTPROP = "outProp";

    private SettingsModelBoolean m_outputProbabilities = createOutputProbabilitiesSettingsModel();

    private SettingsModelBoolean m_changePredColName = PredictorHelper.getInstance().createChangePrediction();

    private SettingsModelString m_predColName = PredictorHelper.getInstance().createPredictionColumn();

    private SettingsModelString m_suffix = PredictorHelper.getInstance().createSuffix();

    /**
     *
     */
    public SparkPMMLPredictorNodeModel() {
        super(new PortType[]{CompiledModelPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * Creates the settings model that determines whether the node should output the probabilities for each class.
     * @return the settings model
     */
    static SettingsModelBoolean createOutputProbabilitiesSettingsModel() {
        return new SettingsModelBoolean(CFG_KEY_OUTPROP, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
//        final CompiledModelPortObjectSpec pmmlSpec = (CompiledModelPortObjectSpec) inSpecs[0];
//        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[1];
//        final DataTableSpec resultSpec = createResultSpec(sparkSpec.getTableSpec(), pmmlSpec);
//        return new PortObjectSpec[] {new SparkDataPortObjectSpec(sparkSpec.getContext(), resultSpec)};
        return new PortObjectSpec[] {null};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final CompiledModelPortObject pmml = (CompiledModelPortObject)inObjects[0];
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[1];
        final String aOutputTableName = SparkIDs.createRDDID();
        final CompiledModelPortObjectSpec cms = (CompiledModelPortObjectSpec)pmml.getSpec();
        final Integer[] colIdxs = SparkPMMLUtil.getColumnIndices(data.getTableSpec(), pmml.getModel());
        final DataTableSpec resultSpec = SparkPMMLUtil.createResultSpec(data.getTableSpec(), cms, colIdxs);
        final SparkDataTable resultRDD = new SparkDataTable(data.getContext(), aOutputTableName, resultSpec);
        final PMMLPredictionTask assignTask = new PMMLPredictionTask();
        assignTask.execute(exec, data.getData(), pmml, colIdxs, m_outputProbabilities.getBooleanValue(), resultRDD);
        return new PortObject[] {new SparkDataPortObject(resultRDD)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_changePredColName.saveSettingsTo(settings);
        m_outputProbabilities.saveSettingsTo(settings);
        m_predColName.saveSettingsTo(settings);
        m_suffix.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        m_changePredColName.loadSettingsFrom(settings);
        m_outputProbabilities.loadSettingsFrom(settings);
        m_predColName.loadSettingsFrom(settings);
        m_suffix.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        m_changePredColName.validateSettings(settings);
        m_outputProbabilities.validateSettings(settings);
        m_predColName.validateSettings(settings);
        m_suffix.validateSettings(settings);
    }
}
