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
package org.knime.bigdata.spark.node.ml.prediction.randomforest;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeSettings;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.FeatureSamplingStrategy;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelNumber;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings class for the Spark Random Forest Tree learner nodes (mllib, ml-classification and ml-regression). Which
 * settings are saved/loaded/validated depends on the provided {@link DecisionTreeLearnerMode}.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class RandomForestLearnerSettings extends DecisionTreeSettings {

    private final SettingsModelInteger m_noOfModels;

    private final SettingsModelString m_featureSamplingStrategy;

    private final SettingsModelBoolean m_shouldSampleData = new SettingsModelBoolean("shouldSampleData", false);

    private SettingsModelDoubleBounded m_dataSamplingRate =
        new SettingsModelDoubleBounded("dataSamplingRate", 1, 0.0000000000001, 1);

    /**
     * Creates a new instance that operates in the given mode.
     *
     * @param mode
     */
    public RandomForestLearnerSettings(final DecisionTreeLearnerMode mode) {
        super(mode);
        if (mode != DecisionTreeLearnerMode.DEPRECATED) {
            m_noOfModels = new SettingsModelIntegerBounded("noOfModels", 5, 1, Integer.MAX_VALUE);
            m_featureSamplingStrategy =
                    new SettingsModelString("featureSamplingStrategy", FeatureSamplingStrategy.auto.name());

            m_shouldSampleData.addChangeListener(new ChangeListener() {
                @Override
                public void stateChanged(final ChangeEvent e) {
                    updateEnabledness();
                }
            });
            m_dataSamplingRate.setEnabled(false);
        } else {
            m_noOfModels = new SettingsModelIntegerBounded("noOfTrees", 5, 1, Integer.MAX_VALUE);
            m_featureSamplingStrategy =
                    new SettingsModelString("featureSubsetStrategy", FeatureSamplingStrategy.auto.name());
        }
    }

    @Override
    protected void updateEnabledness() {
        super.updateEnabledness();
        m_dataSamplingRate.setEnabled(m_shouldSampleData.getBooleanValue());
    }

    /**
     * @return the noOfTrees
     */
    public int getNoOfTrees() {
        return m_noOfModels.getIntValue();
    }

    /**
     * @return the featureSubsetStragegy
     */
    public FeatureSamplingStrategy getFeatureSubsetStragegy() {
        final String stategy = m_featureSamplingStrategy.getStringValue();
        return FeatureSamplingStrategy.valueOf(stategy);
    }

    /**
     * @return the subsampling rate in range (0, 1].
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
     * @return the noOfModelsModel
     */
    public SettingsModelInteger getNoOfModelsModel() {
        return m_noOfModels;
    }


    /**
     * @return the featureSubsetStrategyModel
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
    public SettingsModelNumber getDataSamplingRateModel() {
        return m_dataSamplingRate;
    }

    /**
     * @param tableSpec the original input {@link DataTableSpec}
     * @throws InvalidSettingsException if the settings are invalid
     */
    @Override
    public void check(final DataTableSpec tableSpec) throws InvalidSettingsException {
        super.check(tableSpec);
    }

    /**
     * @param settings the {@link NodeSettingsWO} to write to
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);

        m_noOfModels.saveSettingsTo(settings);
        m_featureSamplingStrategy.saveSettingsTo(settings);
        getSeedModel().saveSettingsTo(settings);

        if (getMode() != DecisionTreeLearnerMode.DEPRECATED) {
            m_shouldSampleData.saveSettingsTo(settings);
            m_dataSamplingRate.saveSettingsTo(settings);
        }
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);

        m_noOfModels.validateSettings(settings);
        m_featureSamplingStrategy.validateSettings(settings);
        getSeedModel().validateSettings(settings);

        if (getMode() != DecisionTreeLearnerMode.DEPRECATED) {
            m_shouldSampleData.validateSettings(settings);
            m_dataSamplingRate.validateSettings(settings);
        }
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettingsFrom(settings);

        m_noOfModels.loadSettingsFrom(settings);
        m_featureSamplingStrategy.loadSettingsFrom(settings);
        getSeedModel().loadSettingsFrom(settings);

        if (getMode() != DecisionTreeLearnerMode.DEPRECATED) {
            m_shouldSampleData.loadSettingsFrom(settings);
            m_dataSamplingRate.loadSettingsFrom(settings);
        }
        updateEnabledness();
    }
}
