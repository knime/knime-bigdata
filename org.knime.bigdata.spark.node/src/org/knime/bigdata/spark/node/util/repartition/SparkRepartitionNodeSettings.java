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
 *   Created on May 6, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.util.repartition;

import org.knime.bigdata.spark.node.util.repartition.RepartitionJobInput.CalculationMode;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings containt for Spark Repartition Node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkRepartitionNodeSettings {

    private static final String ERROR_NEGATIV_FIXED_VALUE = "Fixed value must be greater than zero";

    private static final String ERROR_NEGATIV_FACTOR = "Factor must be greater than zero";

    private final SettingsModelString m_calculationMode =
        new SettingsModelString("calculationMode", CalculationMode.FIXED_VALUE.toString());

    private final SettingsModelIntegerBounded m_fixedValue =
        new SettingsModelIntegerBounded("fixedValue", 20, 1, Integer.MAX_VALUE);

    private final SettingsModelDoubleBounded m_multiplyPartitionFactor =
        new SettingsModelDoubleBounded("multiplyByPartitionFactor", 2.0, 1.0, Integer.MAX_VALUE);

    private final SettingsModelDoubleBounded m_dividePartitionFactor =
        new SettingsModelDoubleBounded("divideByPartitionFactor", 2.0, 1.0, Integer.MAX_VALUE);

    private final SettingsModelDoubleBounded m_multiplyCoresFactor =
        new SettingsModelDoubleBounded("multiplyByCoresFactor", 2.0, 0.000001, Integer.MAX_VALUE);

    private final SettingsModelBoolean m_useCoalesce = new SettingsModelBoolean("useCoalesce", false);

    /**
     * Default constructor
     */
    SparkRepartitionNodeSettings() {
    }

    SettingsModelString getCalculationModeModel() {
        return m_calculationMode;
    }

    /**
     * @return mode to used for new partition count calculation
     */
    CalculationMode getCalculationMode() {
        return CalculationMode.valueOf(m_calculationMode.getStringValue());
    }

    SettingsModelIntegerBounded getFixedValueModel() {
        return m_fixedValue;
    }

    /**
     * @return fixed new partition count
     */
    int getFixedValue() {
        return m_fixedValue.getIntValue();
    }

    SettingsModelDoubleBounded getMultiplyPartitionFactorModel() {
        return m_multiplyPartitionFactor;
    }

    /**
     * @return a factor to multiply current partition count with
     */
    double getMultiplyPartitionFactor() {
        return m_multiplyPartitionFactor.getDoubleValue();
    }

    SettingsModelDoubleBounded getDividePartitionFactorModel() {
        return m_dividePartitionFactor;
    }

    /**
     * @return a factor to divide current partition count by
     */
    double getDividePartitionFactor() {
        return m_dividePartitionFactor.getDoubleValue();
    }

    SettingsModelDoubleBounded getMultiplyCoresFactorModel() {
        return m_multiplyCoresFactor;
    }

    /**
     * @return a factor to multiply count of all available executor cores with
     */
    double getMultiplyCoresFactor() {
        return m_multiplyCoresFactor.getDoubleValue();
    }

    SettingsModelBoolean getUseCoalesceModel() {
        return m_useCoalesce;
    }

    /**
     * @return <code>true</code> if coalesce should be used in case that partition count gets reduced
     */
    boolean useCoalesce() {
        return m_useCoalesce.getBooleanValue();
    }

    /**
     * Saves the the settings of this instance to the given {@link NodeSettingsWO}.
     *
     * @param settings the NodeSettingsWO to write to.
     */
    void saveSettingsTo(final NodeSettingsWO settings) {
        m_calculationMode.saveSettingsTo(settings);
        m_fixedValue.saveSettingsTo(settings);
        m_multiplyPartitionFactor.saveSettingsTo(settings);
        m_dividePartitionFactor.saveSettingsTo(settings);
        m_multiplyCoresFactor.saveSettingsTo(settings);
        m_useCoalesce.saveSettingsTo(settings);
    }

    /**
     * Validates the settings in the given {@link NodeSettingsRO}.
     *
     * @param settings the NodeSettingsRO to validate.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final SparkRepartitionNodeSettings tmp = new SparkRepartitionNodeSettings();
        tmp.loadSettingsFrom(settings);
        tmp.validateSettings();
    }

    /**
     * Validate current settings
     * @throws InvalidSettingsException
     */
    void validateSettings() throws InvalidSettingsException {
        if (getCalculationMode() == CalculationMode.FIXED_VALUE && getFixedValue() <= 0) {
            throw new InvalidSettingsException(ERROR_NEGATIV_FIXED_VALUE + ": " + m_fixedValue);
        } else if (getCalculationMode() == CalculationMode.MULTIPLY_PART_COUNT && getMultiplyPartitionFactor() <= 0) {
            throw new InvalidSettingsException(ERROR_NEGATIV_FACTOR + ": " + m_multiplyPartitionFactor);
        } else if (getCalculationMode() == CalculationMode.DIVIDE_PART_COUNT && getDividePartitionFactor() <= 0) {
            throw new InvalidSettingsException(ERROR_NEGATIV_FACTOR + ": " + m_dividePartitionFactor);
        } else if (getCalculationMode() == CalculationMode.MULTIPLY_PART_COUNT && getMultiplyCoresFactor() <= 0) {
            throw new InvalidSettingsException(ERROR_NEGATIV_FACTOR + ": " + m_multiplyPartitionFactor);
        }
    }

    /**
     * @param settings the NodeSettingsRO to read from.
     * @throws InvalidSettingsException
     */
    void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_calculationMode.loadSettingsFrom(settings);
        m_fixedValue.loadSettingsFrom(settings);
        m_multiplyPartitionFactor.loadSettingsFrom(settings);
        m_dividePartitionFactor.loadSettingsFrom(settings);
        m_multiplyCoresFactor.loadSettingsFrom(settings);
        m_useCoalesce.loadSettingsFrom(settings);
    }
}
