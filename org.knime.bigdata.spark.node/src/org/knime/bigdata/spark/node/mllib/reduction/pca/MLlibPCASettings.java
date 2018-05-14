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
 */
package org.knime.bigdata.spark.node.mllib.reduction.pca;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.core.util.SparkUtil;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnFilter2;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;

/**
 * Settings for Spark PCA node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class MLlibPCASettings {

    private static final String CFG_FAIL_ON_MISSING_VALUES = "failOnMissingValues";
    private static final boolean DEFAULT_FAIL_ON_MISSING_VALUES = false;

    /** Use a fixed value for target number of components */
    protected static final String TARGET_DIM_MODE_FIXED = "fixed";

    /** Use a quality setting (minimum informations fraction to preserver) to calculate the number of components */
    protected static final String TARGET_DIM_MODE_QUALITY = "quality";

    private static final String CFG_TARGET_DIM_MODE = "targetDimensionsMode";
    private static final String DEFAULT_TARGET_DIM_MODE = TARGET_DIM_MODE_QUALITY;

    private static final String CFG_NO_OF_COMPONENTS = "noOfPrincipalComponents";
    private static final int DEFAULT_NO_OF_COMPONENTS = 2;

    private static final String CFG_MIN_QUALITY = "minQuality";
    private static final int DEFAULT_MIN_QUALITY = 100;

    private static final String CFG_FEATURE_COLUMNS = "featureColumns";

    private static final String CFG_REPLACE_INPUT_COLUMNS = "replaceInputColumns";
    private static final boolean DEFAULT_REPLACE_INPUT_COLUMNS = false;

    private final SettingsModelBoolean m_failOnMissingValues =
            new SettingsModelBoolean(CFG_FAIL_ON_MISSING_VALUES, DEFAULT_FAIL_ON_MISSING_VALUES);

    private final SettingsModelString m_targetDimMode =
            new SettingsModelString(CFG_TARGET_DIM_MODE, DEFAULT_TARGET_DIM_MODE);

    private final SettingsModelIntegerBounded m_noOfComponents =
            new SettingsModelIntegerBounded(CFG_NO_OF_COMPONENTS, DEFAULT_NO_OF_COMPONENTS, 1, Integer.MAX_VALUE);

    private final SettingsModelDoubleBounded m_minQuality =
            new SettingsModelDoubleBounded(CFG_MIN_QUALITY, DEFAULT_MIN_QUALITY, 1, 100);

    @SuppressWarnings("unchecked")
    private final SettingsModelColumnFilter2 m_featureColumns =
            new SettingsModelColumnFilter2(CFG_FEATURE_COLUMNS, DoubleValue.class);

    private final SettingsModelBoolean m_replaceInputColumns =
            new SettingsModelBoolean(CFG_REPLACE_INPUT_COLUMNS, DEFAULT_REPLACE_INPUT_COLUMNS);

    private final boolean m_legacyMode;

    /**
     * Default constructor.
     * @param legacyMode <code>true</code> if only number of components and node settings used
     */
    public MLlibPCASettings(final boolean legacyMode) {
        m_legacyMode = legacyMode;
    }

    /**
     * @return <code>true</code> if job should fail on missing values,
     *         <code>false</code> if missing values should be ignored
     */
    public boolean failOnMissingValues() {
        return m_failOnMissingValues.getBooleanValue();
    }

    /** @return fail on missing values settings model */
    public SettingsModelBoolean getFailOnMissingValuesModel() {
        return m_failOnMissingValues;
    }

    /** @return target dimensions mode */
    public String getTargetDimMode() {
        return m_targetDimMode.getStringValue();
    }

    /** @return target dimensions mode settings model */
    public SettingsModelString getTargetDimModeModel() {
        return m_targetDimMode;
    }

    /** @return fixed number of principal components */
    public int getNoOfComponents() {
        return m_noOfComponents.getIntValue();
    }

    /** @return fixed number of principal components settings model */
    public SettingsModelIntegerBounded getNoOfComponentsModel() {
        return m_noOfComponents;
    }

    /** @return minimum quality (in a range between 0.0 and 1.0) */
    public double getMinQuality() {
        return m_minQuality.getDoubleValue() / 100.0;
    }

    /** @return minimum quality settings model */
    public SettingsModelDoubleBounded getMinQualityModel() {
        return m_minQuality;
    }

    /**
     * @param inSpec input table spec
     * @return feature column names
     */
    public String[] getFeatureColumns(final DataTableSpec inSpec) {
        return m_featureColumns.applyTo(inSpec).getIncludes();
    }

    /** @return feature columns settings model */
    public SettingsModelColumnFilter2 getFeatureColumnsModel() {
        return m_featureColumns;
    }

    /**
     * @return <code>true</code> if input columns should be replace,
     *         <code>false</code> if input columns should be included in output table
     */
    public boolean replaceInputColumns() {
        return m_replaceInputColumns.getBooleanValue();
    }

    /** @return replace input columns settings model */
    public SettingsModelBoolean getReplaceInputColumnsModel() {
        return m_replaceInputColumns;
    }

    /**
     * @param inSpec input {@link DataTableSpec} (not <code>null</code>)
     * @return the feature column indices
     * @throws InvalidSettingsException on missing columns
     */
    public Integer[] getFeatureColumnIdices(final DataTableSpec inSpec) throws InvalidSettingsException {
        return SparkUtil.getColumnIndices(inSpec, getFeatureColumns(inSpec));
    }

    /** @param settings settings to save current settings in */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_noOfComponents.saveSettingsTo(settings);
        m_featureColumns.saveSettingsTo(settings);

        if (!m_legacyMode) {
            m_failOnMissingValues.saveSettingsTo(settings);
            m_targetDimMode.saveSettingsTo(settings);
            m_minQuality.saveSettingsTo(settings);
            m_replaceInputColumns.saveSettingsTo(settings);
        }
    }

    /**
     * Validate current settings
     * @param inSpec input {@link DataTableSpec} (not <code>null</code>)
     * @throws InvalidSettingsException on invalid settings
     */
    public void validateSettings(final DataTableSpec inSpec) throws InvalidSettingsException {
        final FilterResult featureColumnsFilterResult = m_featureColumns.applyTo(inSpec);
        final String missingColumns[] = featureColumnsFilterResult.getRemovedFromIncludes();
        final String featureColumns[] = featureColumnsFilterResult.getIncludes();

        if (missingColumns.length == 1) {
            throw new InvalidSettingsException("Input column " + missingColumns[0] + " not found or of wrong type.");
        } else if (missingColumns.length > 1) {
            throw new InvalidSettingsException("Input columns " + StringUtils.join(missingColumns, ", ") + " not found or of wrong type.");
        }

        if (featureColumns.length == 0) {
            throw new InvalidSettingsException("No input columns selected.");
        }

        if (getTargetDimMode().equals(TARGET_DIM_MODE_FIXED) && featureColumns.length < getNoOfComponents()) {
            throw new InvalidSettingsException("Number of target dimension is to high."
                +" Reduce target dimensions to " + featureColumns.length + " or add additional input columns.");
        }
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     * @param settings settings to load
     * @throws InvalidSettingsException on invalid settings
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_noOfComponents.loadSettingsFrom(settings);
        m_featureColumns.loadSettingsFrom(settings);

        if (m_legacyMode) {
            m_failOnMissingValues.setBooleanValue(true);
            m_targetDimMode.setStringValue(TARGET_DIM_MODE_FIXED);
        } else {
            m_failOnMissingValues.loadSettingsFrom(settings);
            m_targetDimMode.loadSettingsFrom(settings);
            m_minQuality.loadSettingsFrom(settings);
            m_replaceInputColumns.loadSettingsFrom(settings);
        }
    }
}
