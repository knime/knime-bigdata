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
 *   Created on 23.08.2015 by koetter
 */
package org.knime.bigdata.spark.node.ml.prediction.gbt;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.core.job.util.EnumContainer;
import org.knime.bigdata.spark.core.job.util.EnumContainer.GBTLossFunction;
import org.knime.bigdata.spark.core.job.util.EnumContainer.LossFunction;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeSettings;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.FeatureSamplingStrategy;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings class for the Spark Gradient-Boosted Tree learner nodes (mllib, ml-classification and ml-regression). Which
 * settings are saved/loaded/validated depends on the provided {@link DecisionTreeLearnerMode}.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class GradientBoostedTreesLearnerSettings extends DecisionTreeSettings {

    private final SettingsModelString m_featureSamplingStrategy =
        new SettingsModelString("featureSamplingStrategy", FeatureSamplingStrategy.all.name());

    private final SettingsModelBoolean m_shouldSampleData = new SettingsModelBoolean("shouldSampleData", false);

    private final SettingsModelDoubleBounded m_dataSamplingRate =
        new SettingsModelDoubleBounded("dataSamplingRate", 1, 0.0000000000001, 1);

    private final SettingsModelString m_lossFunction;

    private final SettingsModelInteger m_maxNoOfModels;

    private final SettingsModelDoubleBounded m_learningRate = new SettingsModelDoubleBounded("learningRate", 0.1, 0, 1);

    /**
     * Creates a new instance that operates in the given mode.
     *
     * @param mode
     */
    public GradientBoostedTreesLearnerSettings(final DecisionTreeLearnerMode mode) {
        super(mode);
        if (mode != DecisionTreeLearnerMode.DEPRECATED) {
            m_lossFunction =
                    new SettingsModelString("lossFunction", EnumContainer.GBTLossFunction.squared.name());

            m_maxNoOfModels = new SettingsModelIntegerBounded("maxModels", 5, 1, Integer.MAX_VALUE);

            m_shouldSampleData.addChangeListener(new ChangeListener() {
                @Override
                public void stateChanged(final ChangeEvent e) {
                    updateEnabledness();
                }
            });
            m_dataSamplingRate.setEnabled(false);
        } else {
            m_lossFunction =
                    new SettingsModelString("lossFunction", EnumContainer.LossFunction.LogLoss.name());
            m_maxNoOfModels = new SettingsModelIntegerBounded("noOfIterations", 5, 1, Integer.MAX_VALUE);
        }
    }

    @Override
    protected void updateEnabledness() {
        super.updateEnabledness();
        m_dataSamplingRate.setEnabled(m_shouldSampleData.getBooleanValue());
    }

    /**
     * @return the featureSubsetStragegy
     */
    public FeatureSamplingStrategy getFeatureSamplingStragegy() {
        final String stategy = m_featureSamplingStrategy.getStringValue();
        return FeatureSamplingStrategy.valueOf(stategy);
    }

    /**
     * @return the fraction of the data to sample per model, in range (0, 1].
     */
    public double getDataSamplingRate() {
        return m_dataSamplingRate.getDoubleValue();
    }

    /**
     * @return whether to sample a fraction of the data per model.
     */
    public boolean shouldSampleData() {
        return m_shouldSampleData.getBooleanValue();
    }

    /**
     * @return the featureSubsetStragegyModel
     */
    public SettingsModelString getFeatureSamplingStrategyModel() {
        return m_featureSamplingStrategy;
    }

    /**
     *
     * @return model for whether to sample a fraction of the data per model.
     */
    public SettingsModelBoolean getShouldSampleDataModel() {
        return m_shouldSampleData;
    }

    /**
     * @return model for the fraction of the training data to use for learning each decision tree.
     */
    public SettingsModelDoubleBounded getDataSamplingRateModel() {
        return m_dataSamplingRate;
    }

    /**
     * @return the maximum number of iterations
     */
    public int getMaxNoOfModels() {
        return m_maxNoOfModels.getIntValue();
    }

    /**
     * @return the {@link LossFunction}
     */
    public LossFunction getLossFunction() {
        final String lossFunction = m_lossFunction.getStringValue();
        return LossFunction.valueOf(lossFunction);
    }

    /**
     * @return the {@link GBTLossFunction}
     */
    public GBTLossFunction getGBTLossFunction() {
        final String lossFunction = m_lossFunction.getStringValue();
        return GBTLossFunction.valueOf(lossFunction);
    }

    /**
     * @return the seed
     */
    public double getLearningRate() {
        return m_learningRate.getDoubleValue();
    }

    /**
     * @return the the maximum number of iterations model
     */
    public SettingsModelInteger getMaxNoOfModelsModel() {
        return m_maxNoOfModels;
    }

    /**
     * @return the featureSubsetStragegyModel
     */
    public SettingsModelString getLossFunctionModel() {
        return m_lossFunction;
    }

    /**
     * @return the seedModel
     */
    public SettingsModelDoubleBounded getLearningRateModel() {
        return m_learningRate;
    }

    /**
     * @param settings the {@link NodeSettingsWO} to write to
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);

        m_maxNoOfModels.saveSettingsTo(settings);
        m_learningRate.saveSettingsTo(settings);

        switch (getMode()) {
            case DEPRECATED:
                m_lossFunction.saveSettingsTo(settings);
                break;
            case CLASSIFICATION:
                m_featureSamplingStrategy.saveSettingsTo(settings);
                m_shouldSampleData.saveSettingsTo(settings);
                m_dataSamplingRate.saveSettingsTo(settings);
                break;
            case REGRESSION:
                m_featureSamplingStrategy.saveSettingsTo(settings);
                m_shouldSampleData.saveSettingsTo(settings);
                m_dataSamplingRate.saveSettingsTo(settings);
                m_lossFunction.saveSettingsTo(settings);
                break;
        }
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);

        m_maxNoOfModels.validateSettings(settings);
        m_learningRate.validateSettings(settings);

        switch (getMode()) {
            case DEPRECATED:
                m_lossFunction.validateSettings(settings);
                break;
            case CLASSIFICATION:
                m_featureSamplingStrategy.validateSettings(settings);
                m_shouldSampleData.validateSettings(settings);
                m_dataSamplingRate.validateSettings(settings);
                break;
            case REGRESSION:
                m_featureSamplingStrategy.validateSettings(settings);
                m_shouldSampleData.validateSettings(settings);
                m_dataSamplingRate.validateSettings(settings);
                m_lossFunction.validateSettings(settings);
                break;
        }
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettingsFrom(settings);

        m_maxNoOfModels.loadSettingsFrom(settings);
        m_learningRate.loadSettingsFrom(settings);

        switch (getMode()) {
            case DEPRECATED:
                m_lossFunction.loadSettingsFrom(settings);
                break;
            case CLASSIFICATION:
                m_featureSamplingStrategy.loadSettingsFrom(settings);
                m_shouldSampleData.loadSettingsFrom(settings);
                m_dataSamplingRate.loadSettingsFrom(settings);
                break;
            case REGRESSION:
                m_featureSamplingStrategy.loadSettingsFrom(settings);
                m_shouldSampleData.loadSettingsFrom(settings);
                m_dataSamplingRate.loadSettingsFrom(settings);
                m_lossFunction.loadSettingsFrom(settings);
                break;
        }
        updateEnabledness();
    }
}
