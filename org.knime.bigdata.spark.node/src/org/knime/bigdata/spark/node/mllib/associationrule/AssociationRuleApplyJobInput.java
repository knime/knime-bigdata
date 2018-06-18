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
 *   Created on Jan 29, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.mllib.associationrule;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Association rules apply job input container.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class AssociationRuleApplyJobInput extends JobInput {
    private static final String RULE_ANTECEDENT_COLUMN = "antecedentColumn";
    private static final String RULE_CONSEQUENT_COLUMN = "consequentColumn";
    private static final String RULE_LIMIT = "ruleLimit";
    private static final String ITEM_COLUMN = "itemColumn";
    private static final String OUTPUT_COLUMN = "outputColumn";

    /** Deserialization constructor. */
    public AssociationRuleApplyJobInput() {}

    AssociationRuleApplyJobInput(final String rulesInputObject, final String antecedentColumn, final String consequentColumn,
        final String itemsInputObject, final String itemColumn, final String outputObject, final String outputColumn) {

        addNamedInputObject(rulesInputObject);
        addNamedInputObject(itemsInputObject);
        addNamedOutputObject(outputObject);

        set(RULE_ANTECEDENT_COLUMN, antecedentColumn);
        set(RULE_CONSEQUENT_COLUMN, consequentColumn);
        set(ITEM_COLUMN, itemColumn);
        set(OUTPUT_COLUMN, outputColumn);
    }

    /** @return rules antecedent column name */
    public String getAntecedentColumn() {
        return get(RULE_ANTECEDENT_COLUMN);
    }

    /** @return rules consequent column name */
    public String getConsequentColumn() {
        return get(RULE_CONSEQUENT_COLUMN);
    }

    /** @param limit maximum number of rules to use */
    public void setRuleLimit(final int limit) {
        set(RULE_LIMIT, limit);
    }

    /** @return <code>true</code> if number of rules to use are limited */
    public boolean hasRuleLimit() {
        return has(RULE_LIMIT);
    }

    /** @return maximum number of rules to use or <code>null</code> (see {@link #hasRuleLimit()}) */
    public int getRuleLimit() {
        return getInteger(RULE_LIMIT);
    }

    /** @return items column name */
    public String getItemColumn() {
        return get(ITEM_COLUMN);
    }

    /** @return output column name */
    public String getOutputColumn() {
        return get(OUTPUT_COLUMN);
    }

    /** @return object id of rules input */
    public String getRulesInputObject() {
        return getNamedInputObjects().get(0);
    }

    /** @return object id of items input */
    public String getItemsInputObject() {
        return getNamedInputObjects().get(1);
    }
}
