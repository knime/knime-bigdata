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
 *   Created on 18.08.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.sampling;

import java.text.NumberFormat;
import java.util.Locale;

import org.knime.base.node.preproc.sample.SamplingNodeSettings;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkSamplingNodeSettings extends SamplingNodeSettings {

    private static final String CFG_WITH_REPLACEMENT = "withReplacement";

    private static final String CFG_EXACT_SAMPLING = "exactSampling";

    private boolean m_withReplacement = false;

    private boolean m_exactSampling = false;

    /**
     * @param withReplacement the withReplacement to set
     */
    public void withReplacement(final boolean withReplacement) {
        m_withReplacement = withReplacement;
    }

    /**
     * @return the withReplacement
     */
    public boolean withReplacement() {
        return m_withReplacement;
    }

    /**
     * @param accurateSampling the accurateSampling to set
     */
    public void exactSampling(final boolean accurateSampling) {
        m_exactSampling = accurateSampling;
    }

    /**
     * @return the accurateSampling
     */
    public boolean exactSampling() {
        return m_exactSampling;
    }

    /**
     * Saves the settings to the given object.
     *
     * @param settings the node settings object
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);
        settings.addBoolean(CFG_WITH_REPLACEMENT, m_withReplacement);
        settings.addBoolean(CFG_EXACT_SAMPLING, m_exactSampling);
    }

    /**
     * Loads the setting from the given object.
     *
     * @param settings the settings
     * @param guessValues If <code>true</code>, default values are used in
     *            case the settings are incomplete, <code>false</code> will
     *            throw an exception. <code>true</code> should be used when
     *            called from the dialog, <code>false</code> when called from
     *            the model.
     * @throws InvalidSettingsException if settings incomplete and
     *             <code>guessValues</code> is <code>false</code>
     */
    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings,
            final boolean guessValues) throws InvalidSettingsException {
        super.loadSettingsFrom(settings, guessValues);
        if (guessValues) {
            m_withReplacement = settings.getBoolean(CFG_WITH_REPLACEMENT, false);
            m_exactSampling = settings.getBoolean(CFG_EXACT_SAMPLING, true);
        } else {
            m_withReplacement = settings.getBoolean(CFG_WITH_REPLACEMENT);
            m_exactSampling = settings.getBoolean(CFG_EXACT_SAMPLING);
        }
    }


    /**
     * @param settings
     * @throws InvalidSettingsException
     */
    public static void validateSamplingSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        SamplingNodeSettings temp = new SamplingNodeSettings();
        temp.loadSettingsFrom(settings, false);

        if (temp.countMethod() == SamplingNodeSettings.CountMethods.Relative) {
            if (temp.fraction() < 0.0 || temp.fraction() > 1.0) {
                NumberFormat f = NumberFormat.getPercentInstance(Locale.US);
                String p = f.format(100.0 * temp.fraction());
                throw new InvalidSettingsException("Invalid percentage: " + p);
            }
        } else if (temp.countMethod() == SamplingNodeSettings.CountMethods.Absolute) {
            if (temp.count() < 0) {
                throw new InvalidSettingsException("Invalid count: "
                        + temp.count());
            }
        } else {
            throw new InvalidSettingsException("Unknown method: "
                    + temp.countMethod());
        }

        if (temp.samplingMethod().equals(SamplingMethods.Stratified)
                && (temp.classColumn() == null)) {
            throw new InvalidSettingsException(
                    "No class column for stratified sampling selected");
        }
    }

    /**
     * Checks if the node settings are valid, i.e. a method has been set and the
     * class column exists if stratified sampling has been chosen.
     *
     * @param inSpec the input table's spec
     * @param settings the {@link SamplingNodeSettings} to check
     * @throws InvalidSettingsException if the settings are invalid
     */
    public static void checkSettings(final DataTableSpec inSpec, final SamplingNodeSettings settings)
            throws InvalidSettingsException {
        if (settings.countMethod() == null) {
            throw new InvalidSettingsException("No sampling method selected");
        }
        if (settings.samplingMethod().equals(SamplingMethods.Stratified)
                && !inSpec.containsName(settings.classColumn())) {
            throw new InvalidSettingsException("Column '"
                    + settings.classColumn() + "' for stratified sampling "
                    + "does not exist");
        }
    }

}
