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
package org.knime.bigdata.spark.node.mllib.prediction.ensemble.randomforest;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentNumberEdit;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;

import org.knime.bigdata.spark.core.job.util.EnumContainer;
import org.knime.bigdata.spark.core.job.util.EnumContainer.FeatureSubsetStrategy;
import org.knime.bigdata.spark.node.mllib.prediction.decisiontree.DecisionTreeComponents;

/**
 *
 * @author Ole Ostergaard, KNIME.com
 */
public class RandomForestComponents extends DecisionTreeComponents<RandomForestSettings> {

    private final DialogComponent m_noOfTreesComponent =
            new DialogComponentNumber(getSettings().getNoOfTreesModel(), "Number of trees", 5, 5);

    private final DialogComponent m_featureSubsetStrategyComponent = new DialogComponentStringSelection(
        getSettings().getFeatureSubsetStragegyModel(), "Feature selection strategy",
        EnumContainer.getNames(FeatureSubsetStrategy.values()));

    private final DialogComponent m_seedComponent = new DialogComponentNumberEdit(getSettings().getSeedModel(), "Seed");

    private final DialogComponent[] m_components =  new DialogComponent[] {m_noOfTreesComponent,
        m_featureSubsetStrategyComponent, m_seedComponent};


    /**
     * Constructor.
     * @param settings the {@link RandomForestSettings}.
     */
    public RandomForestComponents(final RandomForestSettings settings) {
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
     * @return the noOfTreesComponent
     */
    public DialogComponent getNoOfTreesComponent() {
        return m_noOfTreesComponent;
    }

    /**
     * @return the featureSubsetStrategyComponent
     */
    public DialogComponent getFeatureSubsetStrategyComponent() {
        return m_featureSubsetStrategyComponent;
    }

    /**
     * @return the seedComponent
     */
    public DialogComponent getSeedComponent() {
        return m_seedComponent;
    }
}
