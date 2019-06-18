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
 *   Created on May 29, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction.predictor.regression;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Node settings for the spark.ml-based regression predictor.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class MLPredictorRegressionNodeSettings {

    private final SettingsModelBoolean m_overwritePredictionColumn =
        new SettingsModelBoolean("overwritePredictionColumn", false);

    private final SettingsModelString m_predictionColumn = new SettingsModelString("predictionColumn", "Prediction");

    /**
     * Constructor.
     */
    public MLPredictorRegressionNodeSettings() {
        updateEnabledness();

        final ChangeListener changeListener = new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                updateEnabledness();
            }
        };
        m_overwritePredictionColumn.addChangeListener(changeListener);
    }

    private void updateEnabledness() {
        m_predictionColumn.setEnabled(m_overwritePredictionColumn.getBooleanValue());
    }

    /**
     * @return whether to set a non-default prediction column or not.
     */
    public SettingsModelBoolean getOverwritePredictionColumnModel() {
        return m_overwritePredictionColumn;
    }

    /**
     * @return the predictionColumn
     */
    public SettingsModelString getPredictionColumnModel() {
        return m_predictionColumn;
    }

    /**
     * @param settings the NodeSettingsWO to write to.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_overwritePredictionColumn.saveSettingsTo(settings);
        m_predictionColumn.saveSettingsTo(settings);
    }

    /**
     * @param settings the NodeSettingsRO to validate.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_overwritePredictionColumn.validateSettings(settings);
        m_predictionColumn.validateSettings(settings);
    }

    /**
     * @param settings the NodeSettingsRO to read from.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_overwritePredictionColumn.loadSettingsFrom(settings);
        m_predictionColumn.loadSettingsFrom(settings);
    }
}
