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
 *   Created on 29.05.2015 by koetter
 */
package com.knime.bigdata.spark.node.scripting;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 *
 * @author koetter
 */
public class SparkSnippetSettings {
    private final SettingsModelString m_code = createCodeModel();

    /**
     * @return the code model
     */
    static SettingsModelString createCodeModel() {
        return new SettingsModelString("sparkScript", "");
    }

    /**
     * Saves the settings to the given settings object.
     * @param settings to save
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_code.saveSettingsTo(settings);
    }

    /**
     * @param settings to validate
     * @throws InvalidSettingsException if the settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_code.validateSettings(settings);
    }

    /**
     * Loads the settings from the given settings object.
     * @param settings to load from
     * @throws InvalidSettingsException if the settings are invalid
     */
    public void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_code.loadSettingsFrom(settings);
    }

    /**
     * @return the Spark code to execute
     */
    public String getCode() {
        return m_code.getStringValue();
    }
}
