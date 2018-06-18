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

import org.apache.commons.lang3.StringUtils;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
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
public class SparkAssociationRuleApplySettings {

    private static final String CFG_RULE_LIMIT = "ruleLimit";
    private static final int DEFAULT_RULE_LIMIT = 1000;

    private static final String CFG_ANTECEDENT_COLUMN = "antecedentColumn";
    private static final String DEFAULT_ANTECEDENT_COLUMN = "Antecedent";

    private static final String CFG_CONSEQUENT_COLUMN = "consequentColumn";
    private static final String DEFAULT_CONSEQUENT_COLUMN = "Consequent";

    private static final String CFG_ITEM_COLUMN = "itemColumn";
    private static final String DEFAULT_ITEM_COLUMN = "ItemSet";

    private static final String CFG_OUTPUT_COLUMN = "outputColumn";
    private static final String DEFAULT_OUTPUT_COLUMN = "Prediction";

    private final SettingsModelIntegerBounded m_ruleLimit =
        new SettingsModelIntegerBounded(CFG_RULE_LIMIT, DEFAULT_RULE_LIMIT, 1, Integer.MAX_VALUE);

    private final SettingsModelString m_antecedentColumn =
        new SettingsModelString(CFG_ANTECEDENT_COLUMN, DEFAULT_ANTECEDENT_COLUMN);

    private final SettingsModelString m_consequentColumn =
        new SettingsModelString(CFG_CONSEQUENT_COLUMN, DEFAULT_CONSEQUENT_COLUMN);

    private final SettingsModelString m_itemColumn = new SettingsModelString(CFG_ITEM_COLUMN, DEFAULT_ITEM_COLUMN);

    private final SettingsModelString m_outputColumn =
        new SettingsModelString(CFG_OUTPUT_COLUMN, DEFAULT_OUTPUT_COLUMN);

    /** @return <code>true</code> if number of used rules should be limited */
    public boolean hasRuleLimit() {
        return m_ruleLimit.isEnabled();
    }

    /**
     * Optional: Limit the number of rules to use to avoid performance issues. See {@link #hasRuleLimit()}.
     *
     * @return maximal number of rules to use (minimum 1)
     */
    public int getRuleLimit() {
        return m_ruleLimit.getIntValue();
    }

    /** @return rule limit model */
    public SettingsModelIntegerBounded getRuleLimitModel() {
        return m_ruleLimit;
    }

    /** @return name of rule input antecedent column */
    public String getAntecedentColumn() {
        return m_antecedentColumn.getStringValue();
    }

    /** @return antecedent column name model */
    public SettingsModelString getAntecedentColumnModel() {
        return m_antecedentColumn;
    }

    /** @return name of rule input consequent column */
    public String getConsequentColumn() {
        return m_consequentColumn.getStringValue();
    }

    /** @return consequent column name model */
    public SettingsModelString getConsequentColumnModel() {
        return m_consequentColumn;
    }

    /** @return name of items input column name containing a sets of items/transactions */
    public String getItemColumn() {
        return m_itemColumn.getStringValue();
    }

    /** @return item column name model */
    public SettingsModelString getItemColumnModel() {
        return m_itemColumn;
    }

    /** @return name of the output column containing the predicted item sets */
    public String getOutputColumn() {
        return m_outputColumn.getStringValue();
    }

    /** @return output column name model */
    public SettingsModelString getOutputColumnModel() {
        return m_outputColumn;
    }

    /** @param settings settings to save current settings in */
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_ruleLimit.saveSettingsTo(settings);
        m_antecedentColumn.saveSettingsTo(settings);
        m_consequentColumn.saveSettingsTo(settings);
        m_itemColumn.saveSettingsTo(settings);
        m_outputColumn.saveSettingsTo(settings);
    }

    /**
     * @param settings the settings to validate (not <code>null</code>)
     * @throws InvalidSettingsException on invalid settings
     */
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_ruleLimit.validateSettings(settings);
        m_antecedentColumn.validateSettings(settings);
        m_consequentColumn.validateSettings(settings);
        m_itemColumn.validateSettings(settings);
        m_outputColumn.validateSettings(settings);
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     * @param settings the settings to load
     * @throws InvalidSettingsException on invalid settings
     */
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_ruleLimit.loadSettingsFrom(settings);
        m_antecedentColumn.loadSettingsFrom(settings);
        m_consequentColumn.loadSettingsFrom(settings);
        m_itemColumn.loadSettingsFrom(settings);
        m_outputColumn.loadSettingsFrom(settings);
    }

    /**
     * Guess input column names of antecedent, consequent and item columns.
     * @param rulesTableSpec table spec of rules input port
     * @param itemTableSpecs table spec of items input port
     */
    protected void loadDefaults(final DataTableSpec rulesTableSpec, final DataTableSpec itemTableSpecs) {
        loadDefaultsCollection(rulesTableSpec, m_antecedentColumn);
        loadDefaultsCollection(itemTableSpecs, m_itemColumn);

        // guess consequent column via item column type
        if (!rulesTableSpec.containsName(m_consequentColumn.getStringValue())
                && !StringUtils.isBlank(m_itemColumn.getStringValue())
                && itemTableSpecs.containsName(m_itemColumn.getStringValue())) {

            final DataType itemType = itemTableSpecs.getColumnSpec(m_itemColumn.getStringValue()).getType().getCollectionElementType();

            for (int i = 0; i < rulesTableSpec.getNumColumns(); i++) {
                if (rulesTableSpec.getColumnSpec(i).getType().equals(itemType)) {
                    m_consequentColumn.setStringValue(rulesTableSpec.getColumnSpec(i).getName());
                    break;
                }
            }
        }
    }

    /**
     * Finds a collection column in the given table and updates given model if table does not contain column with name
     * from model.
     *
     * @param tableSpec table with possible columns
     * @param columnModel model with collection column name
     */
    public static void loadDefaultsCollection(final DataTableSpec tableSpec, final SettingsModelString columnModel) {
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
