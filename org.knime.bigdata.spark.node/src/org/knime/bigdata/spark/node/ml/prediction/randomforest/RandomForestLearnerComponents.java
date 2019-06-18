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
 *   Created on Jun 15, 2016 by oole
 */
package org.knime.bigdata.spark.node.ml.prediction.randomforest;

import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeComponents;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.DecisionTreeLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.FeatureSamplingStrategy;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class RandomForestLearnerComponents extends DecisionTreeComponents<RandomForestLearnerSettings> {

    private final DialogComponent m_noOfModelsComponent =
            new DialogComponentNumber(getSettings().getNoOfModelsModel(), "", 5, 12);

    /**
     * Only available in classification or regression mode.
     */
    private final DialogComponent m_featureSamplingStrategyComponent = new DialogComponentButtonGroup(
        getSettings().getFeatureSamplingStrategyModel(), null, true, FeatureSamplingStrategy.values());

    /**
     * Only available in classification or regression mode.
     */
    private final DialogComponentBoolean m_shouldSampleDataComponent =
        new DialogComponentBoolean(getSettings().getShouldSampleDataModel(), "Sample fraction of data for each model");

    /**
     * Only available in classification or regression mode.
     */
    private final DialogComponentNumber m_dataSamplingRateComponent =
            new DialogComponentNumber(getSettings().getDataSamplingRateModel(), "", 0.1, 11);

    /**
     * Constructor.
     * @param settings the {@link RandomForestLearnerSettings}.
     */
    public RandomForestLearnerComponents(final RandomForestLearnerSettings settings) {
        super(settings);
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @param tableSpec input {@link DataTableSpec}
     * @throws NotConfigurableException if the settings are invalid
     */
    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec tableSpec)
            throws NotConfigurableException {

        super.loadSettingsFrom(settings, tableSpec);

        m_noOfModelsComponent.loadSettingsFrom(settings, new DataTableSpec[] {tableSpec});
        m_featureSamplingStrategyComponent.loadSettingsFrom(settings, new DataTableSpec[] {tableSpec});
        getSeedComponent().loadSettingsFrom(settings, new DataTableSpec[] {tableSpec});

        if (getMode() != DecisionTreeLearnerMode.DEPRECATED) {
            m_shouldSampleDataComponent.loadSettingsFrom(settings, new DataTableSpec[] {tableSpec});
            m_dataSamplingRateComponent.loadSettingsFrom(settings, new DataTableSpec[] {tableSpec});
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        super.saveSettingsTo(settings);

        m_noOfModelsComponent.saveSettingsTo(settings);
        m_featureSamplingStrategyComponent.saveSettingsTo(settings);
        getSeedComponent().saveSettingsTo(settings);

        if (getMode() != DecisionTreeLearnerMode.DEPRECATED) {
            m_shouldSampleDataComponent.saveSettingsTo(settings);
            m_dataSamplingRateComponent.saveSettingsTo(settings);
        }
    }

    /**
     * @return the noOfTreesComponent
     */
    public DialogComponent getNoOfModelsComponent() {
        return m_noOfModelsComponent;
    }

    /**
     * @return the featureSubsetStrategyComponent
     */
    public DialogComponent getFeatureSamplingStrategyComponent() {
        return m_featureSamplingStrategyComponent;
    }

    /**
     * @return the shouldSampleDataComponent (only available in classification or regression mode)
     */
    public DialogComponentBoolean getShouldSampleDataComponent() {
        return m_shouldSampleDataComponent;
    }

    /**
     * @return the subsamplingRateComponent (only available in classification or regression mode)
     */
    public DialogComponent getDataSamplingRateComponent() {
        return m_dataSamplingRateComponent;
    }
}
