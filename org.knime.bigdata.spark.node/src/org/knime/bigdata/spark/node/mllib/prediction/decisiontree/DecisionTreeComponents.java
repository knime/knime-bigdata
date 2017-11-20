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
package org.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import org.knime.bigdata.spark.core.job.util.EnumContainer;
import org.knime.bigdata.spark.core.job.util.EnumContainer.InformationGain;
import org.knime.bigdata.spark.core.node.MLlibNodeComponents;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;

/**
 *
 * @author Ole Ostergaard, KNIME.com
 * @param <D> {@link DecisionTreeSettings}
 */
public class DecisionTreeComponents<D extends DecisionTreeSettings> extends MLlibNodeComponents<DecisionTreeSettings> {

    private final DialogComponentNumber m_maxDepthComponent =
            new DialogComponentNumber(getSettings().getMaxDepthModel(), "Max depth: ", 5, 5);
    private final DialogComponentNumber m_maxNoOfBinsComponent = new DialogComponentNumber(getSettings().getMaxNoOfBinsModel(),
        "Max number of bins: ", 5, 5);
    private final DialogComponentStringSelection m_qualityMeasureComponent = new DialogComponentStringSelection(
        getSettings().getQualityMeasureModel(), "Quality measure: ",
        EnumContainer.getNames(InformationGain.gini, InformationGain.entropy));
    private final DialogComponent m_isClassificationComponent = new DialogComponentBoolean(getSettings().getIsClassificationModel(),
            "Is classification");
    private final DialogComponent[] m_components =  new DialogComponent[] {m_maxDepthComponent,
        m_maxNoOfBinsComponent, m_qualityMeasureComponent, m_isClassificationComponent};


    /**
     * Constructor.
     * @param settings The extended {@link DecisionTreeSettings}
     */
    public DecisionTreeComponents(final D settings) {
        super(settings);
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
        for (DialogComponent c : m_components) {
            c.loadSettingsFrom(settings, new DataTableSpec[] {tableSpecs});
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
     * @return the maxDepthComponent
     */
    public DialogComponentNumber getMaxDepthComponent() {
        return m_maxDepthComponent;
    }

    /**
     * @return the maxNoOfBinsComponent
     */
    public DialogComponentNumber getMaxNoOfBinsComponent() {
        return m_maxNoOfBinsComponent;
    }

    /**
     * @return the qualityMeasureComponent
     */
    public DialogComponentStringSelection getQualityMeasureComponent() {
        return m_qualityMeasureComponent;
    }

    /**
     * @return the isClassificationComponent
     */
    public DialogComponent getIsClassificationComponent() {
        return m_isClassificationComponent;
    }

    @SuppressWarnings("unchecked")
    @Override
    public D getSettings() {
        return (D)super.getSettings();
    }
}
