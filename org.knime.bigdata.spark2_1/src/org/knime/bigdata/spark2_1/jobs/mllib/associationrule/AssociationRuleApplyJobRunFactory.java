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
package org.knime.bigdata.spark2_1.jobs.mllib.associationrule;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.node.mllib.associationrule.AssociationRuleApplyJobInput;
import org.knime.bigdata.spark.node.mllib.associationrule.AssociationRuleApplyJobOutput;
import org.knime.bigdata.spark.node.mllib.associationrule.SparkAssociationRuleApplyNodeModel;

/**
 * Job run factory of association rules apply job.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class AssociationRuleApplyJobRunFactory extends DefaultJobRunFactory<AssociationRuleApplyJobInput, AssociationRuleApplyJobOutput> {

    /** Default constructor. */
    public AssociationRuleApplyJobRunFactory() {
        super(SparkAssociationRuleApplyNodeModel.JOB_ID, AssociationRuleApplyJob.class, AssociationRuleApplyJobOutput.class);
    }
}
