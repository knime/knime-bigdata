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
package org.knime.bigdata.spark.node.ml.prediction.decisiontree;

import org.knime.bigdata.spark.core.job.util.EnumContainer;
import org.knime.bigdata.spark.core.job.util.EnumContainer.QualityMeasure;
import org.knime.bigdata.spark.core.node.MLlibNodeComponents;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentNumberEdit;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;

/**
 * Provides node dialog components for all model learners based on decision trees.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @param <D> {@link DecisionTreeSettings}
 */
public class DecisionTreeComponents<D extends DecisionTreeSettings> extends MLlibNodeComponents<DecisionTreeSettings> {

    private final DialogComponentNumber m_maxTreeDepthComponent =
        new DialogComponentNumber(getSettings().getMaxTreeDepthModel(), "", 5, 12);

    private final DialogComponentNumber m_maxNoOfBinsComponent =
        new DialogComponentNumber(getSettings().getMaxNoOfBinsModel(), "", 5, 12);

    /**
     * Only available in deprecated or regression mode.
     */
    private final DialogComponentStringSelection m_qualityMeasureComponent =
        new DialogComponentStringSelection(getSettings().getQualityMeasureModel(), "",
            EnumContainer.getNames(QualityMeasure.gini, QualityMeasure.entropy));

    /**
     * Only available in classification or regression mode.
     */
    private final DialogComponentNumber m_minInformationGainComponent =
            new DialogComponentNumber(getSettings().getMinInformationGainModel(), "", 0.1, 11);

    /**
     * Only available in deprecated mode.
     */
    private final DialogComponent m_isClassificationComponent =
        new DialogComponentBoolean(getSettings().getIsClassificationModel(), "");

    /**
     * Only available in classification or regression mode.
     */
    private final DialogComponent m_minRowsPerChildComponent =
        new DialogComponentNumber(getSettings().getMinRowsPerNodeChildModel(), "", 1, 12);

    /**
     * Only available in classification or regression mode.
     */
    private final DialogComponent m_useStaticSeedComponent =
        new DialogComponentBoolean(getSettings().getUseStaticSeedModel(), "Use static random seed");

    /**
     * Only available in classification or regression mode.
     */
    private final DialogComponent m_seedComponent = new DialogComponentNumberEdit(getSettings().getSeedModel(), "", 8);

    /**
     * Constructor.
     * @param settings The extended {@link DecisionTreeSettings}
     */
    public DecisionTreeComponents(final D settings) {
        super(settings, settings.getMode() == DecisionTreeLearnerMode.CLASSIFICATION, false);
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @param tableSpecs input {@link DataTableSpec}
     * @throws NotConfigurableException if the settings are invalid
     */
    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec tableSpecs)
        throws NotConfigurableException {
        super.loadSettingsFrom(settings, tableSpecs);

        m_maxTreeDepthComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpecs});
        m_maxNoOfBinsComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpecs});

        switch (getMode()) {
            case DEPRECATED:
                m_isClassificationComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpecs});
                m_qualityMeasureComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpecs});
                break;
            case CLASSIFICATION:
                m_qualityMeasureComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpecs});
                m_minInformationGainComponent.loadSettingsFrom(settings,  new DataTableSpec[]{tableSpecs});
                m_minRowsPerChildComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpecs});
                m_useStaticSeedComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpecs});
                m_seedComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpecs});
                break;
            case REGRESSION:
                m_minInformationGainComponent.loadSettingsFrom(settings,  new DataTableSpec[]{tableSpecs});
                m_minRowsPerChildComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpecs});
                m_useStaticSeedComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpecs});
                m_seedComponent.loadSettingsFrom(settings, new DataTableSpec[]{tableSpecs});
                break;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        super.saveSettingsTo(settings);

        m_maxTreeDepthComponent.saveSettingsTo(settings);
        m_maxNoOfBinsComponent.saveSettingsTo(settings);

        switch (getMode()) {
            case DEPRECATED:
                m_isClassificationComponent.saveSettingsTo(settings);
                m_qualityMeasureComponent.saveSettingsTo(settings);
                break;
            case CLASSIFICATION:
                m_qualityMeasureComponent.saveSettingsTo(settings);
                m_minInformationGainComponent.saveSettingsTo(settings);
                m_minRowsPerChildComponent.saveSettingsTo(settings);
                m_useStaticSeedComponent.saveSettingsTo(settings);
                m_seedComponent.saveSettingsTo(settings);
                break;
            case REGRESSION:
                m_minInformationGainComponent.saveSettingsTo(settings);
                m_minRowsPerChildComponent.saveSettingsTo(settings);
                m_useStaticSeedComponent.saveSettingsTo(settings);
                m_seedComponent.saveSettingsTo(settings);
                break;
        }
    }

    /**
     * @return the maxDepthComponent
     */
    public DialogComponentNumber getMaxDepthComponent() {
        return m_maxTreeDepthComponent;
    }

    /**
     * @return the maxNoOfBinsComponent
     */
    public DialogComponentNumber getMaxNoOfBinsComponent() {
        return m_maxNoOfBinsComponent;
    }

    /**
     * @return the qualityMeasureComponent (only available in classification or deprecated mode)
     */
    public DialogComponentStringSelection getQualityMeasureComponent() {
        return m_qualityMeasureComponent;
    }

    /**
     * @return the minInformationGainComponent (only available in classification or regression mode)
     */
    public DialogComponent getMinInformationGainComponent() {
        return m_minInformationGainComponent;
    }

    /**
     * @return the isClassificationComponent (only available in deprecated mode)
     */
    public DialogComponent getIsClassificationComponent() {
        return m_isClassificationComponent;
    }

    /**
     * @return the minRowsPerChildComponent (only available in classification or regression mode)
     */
    public DialogComponent getMinRowsPerChildComponent() {
        return m_minRowsPerChildComponent;
    }

    /**
     * @return the useStaticSeedComponent (only available in classification or regression mode)
     */
    public DialogComponent getUseStaticSeedComponent() {
        return m_useStaticSeedComponent;
    }

    /**
     * @return the seedComponent (only available in classification or regression mode)
     */
    public DialogComponent getSeedComponent() {
        return m_seedComponent;
    }

    @SuppressWarnings("unchecked")
    @Override
    public D getSettings() {
        return (D)super.getSettings();
    }

    /**
     *
     * @return the current mode of the learner node (deprecated, classification, regression)
     */
    protected DecisionTreeLearnerMode getMode() {
        return getSettings().getMode();
    }

}
