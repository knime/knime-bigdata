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
 *   Created on Jan 30, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.mllib.freqitemset;

import org.knime.bigdata.spark.node.mllib.associationrule.SparkAssociationRuleApplySettings;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Frequent item sets settings container.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkFrequentItemSetSettings {

    private static final String CFG_ITEM_COLUMN = "itemColumn";
    private static final String DEFAULT_ITEM_COLUMN = "items";

    private static final String CFG_MIN_SUPPORT = "minSupport";
    private static final double DEFAULT_MIN_SUPPORT = 0.3;

    private static final String CFG_NUM_PARTITIONS = "numPartitions";
    private static final int DEFAULT_NUM_PARTITIONS = 20;

    private final SettingsModelString m_itemColumn = new SettingsModelString(CFG_ITEM_COLUMN, DEFAULT_ITEM_COLUMN);

    private final SettingsModelDoubleBounded m_minSupportModel =
        new SettingsModelDoubleBounded(CFG_MIN_SUPPORT, DEFAULT_MIN_SUPPORT, 0.0, 1.0);

    private final SettingsModelIntegerBounded m_numPartitionsModel =
        new SettingsModelIntegerBounded(CFG_NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS, 1, 10000);

    /** @return name of input column that contains sets of items/transactions. */
    public String getItemsColumn() {
        return m_itemColumn.getStringValue();
    }

    /** @return item set column name model */
    public SettingsModelString getItemsColumnModel() {
        return m_itemColumn;
    }

    /**
     * Minimal support level of the frequent pattern in [0.0, 1.0]. Any pattern that appears
     * more than (minSupport * size-of-the-dataset) times will be output in the frequent itemsets.
     * @return minimum support level in [0.0, 1.0]
     */
    public double getMinSupport() {
        return m_minSupportModel.getDoubleValue();
    }

    /** @return minimum support level model */
    public SettingsModelDoubleBounded getMinSupportModel() {
        return m_minSupportModel;
    }

    /** @return <code>false</code> if partition number of the input dataset should be used */
    public boolean overwriteNumPartitions() {
        return m_numPartitionsModel.isEnabled();
    }

    /**
     * Optional: Number of partitions (at least 1) used by parallel FP-growth. By default the param is not set, and
     * partition number of the input dataset is use (see {@link #overwriteNumPartitions()}).
     *
     * @return number of partitions to use in (minimum 1)
     */
    public int getNumPartitions() {
        return m_numPartitionsModel.getIntValue();
    }

    /** @return number of partitions to use model */
    public SettingsModelIntegerBounded getNumPartitionsModel() {
        return m_numPartitionsModel;
    }

    /** Default constructor. */
    public SparkFrequentItemSetSettings() {
        m_numPartitionsModel.setEnabled(false); // disabled by default
    }

    /** @param settings settings to save current settings in */
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_itemColumn.saveSettingsTo(settings);
        m_minSupportModel.saveSettingsTo(settings);
        m_numPartitionsModel.saveSettingsTo(settings);
    }

    /**
     * @param settings the settings to validate (not <code>null</code>)
     * @throws InvalidSettingsException on invalid settings
     */
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_itemColumn.validateSettings(settings);
        m_minSupportModel.validateSettings(settings);
        m_numPartitionsModel.validateSettings(settings);
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     * @param settings the settings to load
     * @throws InvalidSettingsException on invalid settings
     */
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_itemColumn.loadSettingsFrom(settings);
        m_minSupportModel.loadSettingsFrom(settings);
        m_numPartitionsModel.loadSettingsFrom(settings);
    }

    /**
     * Guess input column containing sets of items/transactions.
     * @param itemTableSpecs table spec of items input port
     */
    public void loadDefaults(final DataTableSpec itemTableSpecs) {
        SparkAssociationRuleApplySettings.loadDefaultsCollection(itemTableSpecs, m_itemColumn);
    }
}
