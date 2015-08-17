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
 *   Created on 03.08.2015 by dwk
 */
package com.knime.bigdata.spark.node.preproc.normalize;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.core.node.util.filter.column.DataTypeColumnFilter;

/**
 *
 * @author dwk
 * @note this is an identical copy of org.knime.base.node.preproc.normalize3.Normalizer3Config (hopefully), we had to
 *       copy it as it is package protected TODO - make Normalizer3Config public and remove this class
 */
public class SparkNormalizerPMMLConfig {

    /** Key to store the new minimum value (in min/max mode). */
    private static final String NEWMIN_KEY = "new-min";

    /** Key to store the new maximum value (in min/max mode). */
    private static final String NEWMAX_KEY = "new-max";

    /** Key to store the mode. */
    private static final String MODE_KEY = "mode";

    /** Key to store the columns to use. */
    private static final String COLUMNS_KEY = "data-column-filter";

    /** Default minimum zero. */
    private double m_min = 0;

    /** Default maximum one. */
    private double m_max = 1;

    /** Normalizer mode. */
    private NormalizerMode m_mode = NormalizerMode.MINMAX;

    @SuppressWarnings("unchecked")
    private DataColumnSpecFilterConfiguration m_dataColumnFilterConfig = new DataColumnSpecFilterConfiguration(
        COLUMNS_KEY, new DataTypeColumnFilter(DoubleValue.class));

    /**
     * @return the min
     */
    double getMin() {
        return m_min;
    }

    /**
     * @param min the min to set
     */
    void setMin(final double min) {
        m_min = min;
    }

    /**
     * @return the max
     */
    double getMax() {
        return m_max;
    }

    /**
     * @param max the max to set
     */
    void setMax(final double max) {
        m_max = max;
    }

    /**
     * @return the mode
     */
    NormalizerMode getMode() {
        return m_mode;
    }

    /**
     * @param mode the mode to set
     */
    void setMode(final NormalizerMode mode) {
        m_mode = mode;
    }

    /**
     * @return the dataColumnFilterConfig
     */
    DataColumnSpecFilterConfiguration getDataColumnFilterConfig() {
        return m_dataColumnFilterConfig;
    }

    /**
     * Loads the configuration for the dialog with corresponding default values.
     *
     * @param settings the settings to load
     * @param spec the data column spec
     */
    void loadConfigurationInDialog(final NodeSettingsRO settings, final DataTableSpec spec) {
        m_mode = NormalizerMode.valueOf(settings.getString(MODE_KEY, NormalizerMode.MINMAX.toString()));
        m_min = settings.getDouble(NEWMIN_KEY, 0.0);
        m_max = settings.getDouble(NEWMAX_KEY, 1.0);
        m_dataColumnFilterConfig.loadConfigurationInDialog(settings, spec);
    }

    /**
     * Loads the configuration for the model.
     *
     * @param settings the settings to load
     * @throws InvalidSettingsException if the settings are invalid
     */
    void loadConfigurationInModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_mode = NormalizerMode.valueOf(settings.getString(MODE_KEY));

        if (NormalizerMode.MINMAX.equals(m_mode)) {
            m_min = settings.getDouble(NEWMIN_KEY);
            m_max = settings.getDouble(NEWMAX_KEY);
        }

        checkSetting(!NormalizerMode.MINMAX.equals(m_mode) || m_min < m_max,
            "Max cannot be smaller than the min value.");
        m_dataColumnFilterConfig.loadConfigurationInModel(settings);
    }

    /**
     * @param spec the table spec
     */
    void guessDefaults(final DataTableSpec spec) {
        m_dataColumnFilterConfig.loadDefaults(spec, true);
        m_mode = NormalizerMode.MINMAX;
    }

    /**
     * Called from dialog's and model's save method.
     *
     * @param settings Arg settings.
     */
    void saveSettings(final NodeSettingsWO settings) {
        settings.addString(MODE_KEY, m_mode.toString());
        settings.addDouble(NEWMIN_KEY, m_min);
        settings.addDouble(NEWMAX_KEY, m_max);
        m_dataColumnFilterConfig.saveConfiguration(settings);
    }

    /**
     * Throws an {@link InvalidSettingsException} with the given string template, if the given predicate is
     * <code>false</code>.
     *
     * @param predicate the predicate
     * @param template the template
     * @throws InvalidSettingsException
     */
    private static void checkSetting(final boolean predicate, final String template, final Object... args)
        throws InvalidSettingsException {
        if (!predicate) {
            throw new InvalidSettingsException(String.format(template, args));
        }
    }

    /**
     * Normalization Mode.
     *
     * @author Marcel Hanser
     */
    enum NormalizerMode {
        /**
         * Z-Score.
         */
        Z_SCORE,
        /**
         * Decimal Scaling.
         */
        DECIMALSCALING,
        /**
         * Min-Max normalization.
         */
        MINMAX;
    }

}
