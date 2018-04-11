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

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Frequent item sets settings container.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SuppressWarnings("javadoc")
public class SparkFrequentItemSetSettings {

    /** Input column name containing a collection of items/transactions. */
    private static final String CFG_ITEM_COLUMN = "itemColumn";
    private static final String DEFAULT_ITEM_COLUMN = "items";
    private final SettingsModelString m_itemColumn = new SettingsModelString(CFG_ITEM_COLUMN, DEFAULT_ITEM_COLUMN);
    public String getItemsColumn() { return m_itemColumn.getStringValue(); }
    public SettingsModelString getItemsColumnModel() { return m_itemColumn; }

    /**
     * Minimal support level of the frequent pattern. [0.0, 1.0]. Any pattern that appears
     * more than (minSupport * size-of-the-dataset) times will be output in the frequent itemsets.
     */
    private static final String CFG_MIN_SUPPORT = "minSupport";
    private static final double DEFAULT_MIN_SUPPORT = 0.3;
    private final SettingsModelDoubleBounded m_minSupportModel = new SettingsModelDoubleBounded(CFG_MIN_SUPPORT, DEFAULT_MIN_SUPPORT, 0.0, 1.0);
    public double getMinSupport() { return m_minSupportModel.getDoubleValue(); }
    public SettingsModelDoubleBounded getMinSupportModel() { return m_minSupportModel; }

    /**
     * Number of partitions (at least 1) used by parallel FP-growth. By default the param is not
     * set, and partition number of the input dataset is used.
     */
    private static final String CFG_NUM_PARTITIONS = "numPartitions";
    private static final int DEFAULT_NUM_PARTITIONS = 20;
    private final SettingsModelInteger m_numPartitionsModel = new SettingsModelInteger(CFG_NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
    public boolean overwriteNumPartitions() { return m_numPartitionsModel.isEnabled(); }
    public int getNumPartitions() { return m_numPartitionsModel.getIntValue(); }
    public SettingsModelInteger getNumPartitionsModel() { return m_numPartitionsModel; }

    /** Default constructor. */
    public SparkFrequentItemSetSettings() {
        m_numPartitionsModel.setEnabled(false); // disabled by default
    }

    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_itemColumn.saveSettingsTo(settings);
        m_minSupportModel.saveSettingsTo(settings);
        m_numPartitionsModel.saveSettingsTo(settings);
    }

    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_itemColumn.validateSettings(settings);
        m_minSupportModel.validateSettings(settings);
        m_numPartitionsModel.validateSettings(settings);
    }

    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_itemColumn.loadSettingsFrom(settings);
        m_minSupportModel.loadSettingsFrom(settings);
        m_numPartitionsModel.loadSettingsFrom(settings);
    }

    protected void loadDefaults(final DataTableSpec tableSpecs[]) {
        loadDefaults(tableSpecs[0], m_itemColumn);
    }

    /**
     * Finds a collection column in the given table and updates given model if table does not contain column with name
     * from model.
     *
     * @param tableSpec table with possible columns
     * @param columnModel model with collection column name
     */
    protected void loadDefaults(final DataTableSpec tableSpec, final SettingsModelString columnModel) {
        if (!tableSpec.containsName(columnModel.getStringValue())
                || !tableSpec.getColumnSpec(columnModel.getStringValue()).getType().isCollectionType()) {

            for (int i = 0; i < tableSpec.getNumColumns(); i++) {
                if (tableSpec.getColumnSpec(i).getType().isCollectionType()) {
                    columnModel.setStringValue(tableSpec.getColumnSpec(i).getName());
                    break;
                }
            }
        }
    }
}
