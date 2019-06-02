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
package org.knime.bigdata.spark.node.ml.prediction.predictor.classification;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class MLPredictorClassificationNodeSettings {

    private final SettingsModelBoolean m_overwritePredictionColumn =
        new SettingsModelBoolean("overwritePredictionColumn", false);

    private final SettingsModelString m_predictionColumn = new SettingsModelString("predictionColumn", "Prediction");

    private final SettingsModelBoolean m_appendClassProbabilityColumns =
        new SettingsModelBoolean("appendClassProbabilityColumns", false);

    private final SettingsModelString m_probabilityColumnSuffix =
        new SettingsModelString("probabilityColumnSuffix", "");

    public MLPredictorClassificationNodeSettings() {
        updateEnabledness();

        final ChangeListener changeListener = new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                updateEnabledness();
            }
        };
        m_overwritePredictionColumn.addChangeListener(changeListener);
        m_appendClassProbabilityColumns.addChangeListener(changeListener);
    }

    private void updateEnabledness() {
        m_predictionColumn.setEnabled(m_overwritePredictionColumn.getBooleanValue());
        m_probabilityColumnSuffix.setEnabled(m_appendClassProbabilityColumns.getBooleanValue());
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
     * @return the appendClassProbabilityColumns
     */
    public SettingsModelBoolean getAppendClassProbabilityColumnsModel() {
        return m_appendClassProbabilityColumns;
    }

    /**
     * @return the probabilityColumnSuffix
     */
    public SettingsModelString getProbabilityColumnSuffixModel() {
        return m_probabilityColumnSuffix;
    }

    /**
     * @param settings the NodeSettingsWO to write to.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_overwritePredictionColumn.saveSettingsTo(settings);
        m_predictionColumn.saveSettingsTo(settings);
        m_appendClassProbabilityColumns.saveSettingsTo(settings);
        m_probabilityColumnSuffix.saveSettingsTo(settings);
    }

    /**
     * @param settings the NodeSettingsRO to validate.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_overwritePredictionColumn.validateSettings(settings);
        m_predictionColumn.validateSettings(settings);
        m_appendClassProbabilityColumns.validateSettings(settings);
        m_probabilityColumnSuffix.validateSettings(settings);
    }

    /**
     * @param settings the NodeSettingsRO to read from.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_overwritePredictionColumn.loadSettingsFrom(settings);
        m_predictionColumn.loadSettingsFrom(settings);
        m_appendClassProbabilityColumns.loadSettingsFrom(settings);
        m_probabilityColumnSuffix.loadSettingsFrom(settings);
    }
}
