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

import java.io.Serializable;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Association rules apply job input container.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
@SuppressWarnings("javadoc")
public class AssociationRuleApplyJobInput extends JobInput {
    private static final String RULE_MODEL = "rulesModel";
    private static final String RULE_LIMIT = "ruleLimit";
    private static final String ITEM_COLUMN = "itemColumn";
    private static final String OUTPUT_COLUMN = "outputColumn";

    /** Deserialization constructor. */
    public AssociationRuleApplyJobInput() {}

    AssociationRuleApplyJobInput(final Serializable rulesModel, final String itemsInputObject, final String outputObject,
        final String itemColumn, final String outputColumn) {

        addNamedInputObject(itemsInputObject);
        addNamedOutputObject(outputObject);
        set(RULE_MODEL, rulesModel);
        set(ITEM_COLUMN, itemColumn);
        set(OUTPUT_COLUMN, outputColumn);
    }


    /** @return association rule model */
    public Serializable getRuleModel() {
        return get(RULE_MODEL);
    }

    public void setRuleLimit(final int limit) {
        set(RULE_LIMIT, limit);
    }

    public boolean hasRuleLimit() {
        return has(RULE_LIMIT);
    }

    /** @return maximal number of rules to use or null (see {@link #hasRuleLimit()}) */
    public int getRuleLimit() {
        return getInteger(RULE_LIMIT);
    }

    public String getItemColumn() {
        return get(ITEM_COLUMN);
    }

    public String getOutputColumn() {
        return get(OUTPUT_COLUMN);
    }

    public String getItemsInputObject() {
        return getFirstNamedInputObject();
    }
}
