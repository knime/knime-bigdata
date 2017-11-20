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
package org.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;

import org.knime.bigdata.spark.core.job.util.EnumContainer;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.DecisionTreeComponents;

/**
 *
 * @author Ole Ostergaard, KNIME.com
 */
public class GradientBoostedTreesComponents extends DecisionTreeComponents<GradientBoostedTreesSettings> {

    private final DialogComponent m_noOfIterationsComponent =
            new DialogComponentNumber(getSettings().getNoOfIterationsModel(), "Number of iterations", 5, 5);

    private final DialogComponent m_lossFunctionComponent = new DialogComponentStringSelection(
        getSettings().getLossFunctionModel(), "Loss function", EnumContainer.getNames(EnumContainer.LossFunction.values()));

    private final DialogComponent m_learningRateComponent =
            new DialogComponentNumber(getSettings().getLearningRateModel(), "Learning rate", 0.01);

    private final DialogComponent[] m_components =  new DialogComponent[] {m_noOfIterationsComponent,
        m_lossFunctionComponent, m_learningRateComponent};

    /**
     * Constructor.
     * @param settings the {@link GradientBoostedTreesSettings}.
     */
    public GradientBoostedTreesComponents(final GradientBoostedTreesSettings settings) {
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
        for (DialogComponent c : m_components) {
            c.loadSettingsFrom(settings, new DataTableSpec[] {tableSpec});
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        super.saveSettingsTo(settings);
        for (DialogComponent c: m_components) {
            c.saveSettingsTo(settings);
        }
    }

    /**
     * @return the noOfIterationsComponent
     */
    public DialogComponent getNoOfIterationsComponent() {
        return m_noOfIterationsComponent;
    }

    /**
     * @return the lossFunctionComponent
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
}