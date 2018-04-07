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
package org.knime.bigdata.spark.node.mllib.associationrule;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Association rules apply settings container.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SuppressWarnings("javadoc")
public class AssociationRuleApplySettings {

    /** Optional: limit the number of rules to use to avoid performance issues. */
    private static final String CFG_RULE_LIMIT = "ruleLimit";
    private static final int DEFAULT_RULE_LIMIT = 1000;
    private final SettingsModelIntegerBounded m_ruleLimit = new SettingsModelIntegerBounded(CFG_RULE_LIMIT, DEFAULT_RULE_LIMIT, 1, Integer.MAX_VALUE);
    public boolean hasRuleLimit() { return m_ruleLimit.isEnabled(); }
    public int getRuleLimit() { return m_ruleLimit.getIntValue(); }
    public SettingsModelIntegerBounded getRuleLimitModel() { return m_ruleLimit; }

    /** Input column name containing a collection of items. */
    private static final String CFG_ITEM_COLUMN = "itemColumn";
    private static final String DEFAULT_ITEM_COLUMN = "items";
    private final SettingsModelString m_itemColumn = new SettingsModelString(CFG_ITEM_COLUMN, DEFAULT_ITEM_COLUMN);
    public String getItemColumn() { return m_itemColumn.getStringValue(); }
    public SettingsModelString getItemColumnModel() { return m_itemColumn; }

    /** Output column name containing the predicted items collection. */
    private static final String CFG_OUTPUT_COLUMN = "outputColumn";
    private static final String DEFAULT_OUTPUT_COLUMN = "prediction";
    private final SettingsModelString m_outputColumn = new SettingsModelString(CFG_OUTPUT_COLUMN, DEFAULT_OUTPUT_COLUMN);
    public String getOutputColumn() { return m_outputColumn.getStringValue(); }
    public SettingsModelString getOutputColumnModel() { return m_outputColumn; }

    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_ruleLimit.saveSettingsTo(settings);
        m_itemColumn.saveSettingsTo(settings);
        m_outputColumn.saveSettingsTo(settings);
    }

    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_ruleLimit.validateSettings(settings);
        m_itemColumn.validateSettings(settings);
        m_outputColumn.validateSettings(settings);
    }

    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_ruleLimit.loadSettingsFrom(settings);
        m_itemColumn.loadSettingsFrom(settings);
        m_outputColumn.loadSettingsFrom(settings);
    }

    protected void loadDefaults(final DataTableSpec itemTableSpecs) {
        loadDefaults(itemTableSpecs, m_itemColumn);
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
