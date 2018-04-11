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
import org.knime.bigdata.spark.node.mllib.associationrule.AssociationRuleLearnerJobInput;
import org.knime.bigdata.spark.node.mllib.associationrule.AssociationRuleLearnerJobOutput;
import org.knime.bigdata.spark.node.mllib.associationrule.SparkAssociationRuleLearnerNodeModel;

/**
 * Job run factory of association rules learner job.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class AssociationRuleLearnerJobRunFactory extends DefaultJobRunFactory<AssociationRuleLearnerJobInput, AssociationRuleLearnerJobOutput> {

    /** Default constructor. */
    public AssociationRuleLearnerJobRunFactory() {
        super(SparkAssociationRuleLearnerNodeModel.JOB_ID, AssociationRuleLearnerJob.class, AssociationRuleLearnerJobOutput.class);
    }
}
