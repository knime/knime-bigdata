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

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Association rules apply job output container.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
@SuppressWarnings("javadoc")
public class AssociationRuleApplyJobOutput extends JobOutput {
    private static final String RULE_COUNT = "ruleCount";

    /** Deserialization constructor */
    public AssociationRuleApplyJobOutput() {}

    public AssociationRuleApplyJobOutput(final int ruleCount) {
        set(RULE_COUNT, ruleCount);
    }

    public int getRuleCount() {
        return getInteger(RULE_COUNT);
    }
}
