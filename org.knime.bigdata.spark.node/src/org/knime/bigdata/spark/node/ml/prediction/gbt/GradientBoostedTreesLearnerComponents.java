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
package org.knime.bigdata.spark.node.ml.prediction.gbt;

import org.knime.bigdata.spark.core.job.util.EnumContainer;
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
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class GradientBoostedTreesLearnerComponents extends DecisionTreeComponents<GradientBoostedTreesLearnerSettings> {

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


    private final DialogComponent m_maxNoOfModelsComponent =
            new DialogComponentNumber(getSettings().getMaxNoOfModelsModel(), "", 5, 12);

    /**
     * Only available in deprecated or regression mode.
     */
    private final DialogComponent m_lossFunctionComponent;

    private final DialogComponent m_learningRateComponent =
            new DialogComponentNumber(getSettings().getLearningRateModel(), "", 0.01, 11);

    /**
     * Constructor.
     *
     * @param settings the {@link GradientBoostedTreesLearnerSettings}.
     */
    public GradientBoostedTreesLearnerComponents(final GradientBoostedTreesLearnerSettings settings) {
        super(settings);

        if (getMode() == DecisionTreeLearnerMode.DEPRECATED) {
            m_lossFunctionComponent = new DialogComponentStringSelection(getSettings().getLossFunctionModel(), "",
                EnumContainer.getNames(EnumContainer.LossFunction.values()));
        } else {
            m_lossFunctionComponent = new DialogComponentStringSelection(getSettings().getLossFunctionModel(), "",
                EnumContainer.getNames(EnumContainer.GBTLossFunction.values()));
        }
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

        m_maxNoOfModelsComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpec});
        m_learningRateComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpec});

        switch (getMode()) {
            case DEPRECATED:
                m_lossFunctionComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpec});
                break;
            case CLASSIFICATION:
                m_featureSamplingStrategyComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpec});
                m_shouldSampleDataComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpec});
                m_dataSamplingRateComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpec});
                break;
            case REGRESSION:
                m_featureSamplingStrategyComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpec});
                m_shouldSampleDataComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpec});
                m_dataSamplingRateComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpec});
                m_lossFunctionComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpec});
                break;
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        super.saveSettingsTo(settings);

        m_maxNoOfModelsComponent.saveSettingsTo(settings);
        m_learningRateComponent.saveSettingsTo(settings);

        switch (getMode()) {
            case DEPRECATED:
                m_lossFunctionComponent.saveSettingsTo(settings);
                break;
            case CLASSIFICATION:
                m_featureSamplingStrategyComponent.saveSettingsTo(settings);
                m_shouldSampleDataComponent.saveSettingsTo(settings);
                m_dataSamplingRateComponent.saveSettingsTo(settings);
                break;
            case REGRESSION:
                m_featureSamplingStrategyComponent.saveSettingsTo(settings);
                m_shouldSampleDataComponent.saveSettingsTo(settings);
                m_dataSamplingRateComponent.saveSettingsTo(settings);
                m_lossFunctionComponent.saveSettingsTo(settings);
                break;
        }
    }

    /**
     * @return the maxIterationsComponent
     */
    public DialogComponent getMaxNoOfModelsComponent() {
        return m_maxNoOfModelsComponent;
    }

    /**
     * @return the lossFunctionComponent (only available in deprecated or regression mode)
     */
    public DialogComponent getLossFunctionComponent() {
        return m_lossFunctionComponent;
    }

    /**
     * @return the learningRateComponent
     */
    public DialogComponent getLearningRateComponent() {
        return m_learningRateComponent;
    }

    /**
     * @return the featureSubsetStrategyComponent (only available in classification or regression mode)
     */
    public DialogComponent getFeatureSubsetStrategyComponent() {
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