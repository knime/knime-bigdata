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
package org.knime.bigdata.spark.core.node;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter2;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;

/**
 *  Holds the Components for the {@link MLlibNodeSettings}.
 *
 * @author Ole Ostergaard, KNIME.com
 * @param <T> The extended {@link MLlibNodeSettings}.
 */
public class MLlibNodeComponents<T extends MLlibNodeSettings> {

    private final T m_nodeSettings;

    private final DialogComponent m_classColComponent;

    /*Access the feature columns component only via the getter method since it is generated only when necessary
     * to prevent NullPointerExceptions in loadSettingsFrom method due to change listener and missing table spec.*/
    private DialogComponent m_featureColsComponent;

    /**
     * @param nodeSettings the extended {@link MLlibNodeSettings}
     */
    @SuppressWarnings("unchecked")
    public MLlibNodeComponents(final T nodeSettings) {
        m_nodeSettings = nodeSettings;
        m_classColComponent = new DialogComponentColumnNameSelection(m_nodeSettings.getClassColModel(),
            "Class column ", 0, DoubleValue.class);
    }

    /**
     * @return the classColComponent
     */
    public DialogComponent getClassColComponent() {
        return m_classColComponent;
    }

    /**
     * @return the colsComponent
     */
    public DialogComponent getFeatureColsComponent() {
        if (m_featureColsComponent == null) {
            m_featureColsComponent = new DialogComponentColumnFilter2(m_nodeSettings.getFeatureColsModel(), 0);
        }
        return m_featureColsComponent;
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @param tableSpecs input {@link DataTableSpec}
     * @throws NotConfigurableException if the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec tableSpecs)
            throws NotConfigurableException {
        if (m_nodeSettings.isRequiresClassCol()) {
            m_classColComponent.loadSettingsFrom(settings, new DataTableSpec[] {tableSpecs});
        }
       m_featureColsComponent.loadSettingsFrom(settings, new DataTableSpec[] {tableSpecs});
    }

    /**
     * @param settings The {@link NodeSettingsWO} to write to
     * @throws InvalidSettingsException If the settings are invalid
     */
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
    	m_classColComponent.saveSettingsTo(settings);
    	m_featureColsComponent.saveSettingsTo(settings);
    }

    /**
     * @return The components underlying {@link MLlibNodeSettings}.
     */
    public T getSettings() {
        return m_nodeSettings;
    }

}
