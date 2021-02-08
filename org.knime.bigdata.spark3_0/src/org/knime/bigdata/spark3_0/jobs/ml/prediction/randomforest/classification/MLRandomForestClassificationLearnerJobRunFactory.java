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
 *   Created on 29.01.2016 by koetter
 */
package org.knime.bigdata.spark3_0.jobs.ml.prediction.randomforest.classification;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.core.job.MLModelLearnerJobOutput;
import org.knime.bigdata.spark.node.ml.prediction.decisiontree.classification.MLDecisionTreeClassificationLearnerJobInput;
import org.knime.bigdata.spark.node.ml.prediction.randomforest.classification.MLRandomForestClassificationLearnerNodeModel;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class MLRandomForestClassificationLearnerJobRunFactory
    extends DefaultJobRunFactory<MLDecisionTreeClassificationLearnerJobInput, MLModelLearnerJobOutput> {

    /** Constructor. */
    public MLRandomForestClassificationLearnerJobRunFactory() {
        super(MLRandomForestClassificationLearnerNodeModel.JOB_ID, MLRandomForestClassificationLearnerJob.class,
            MLModelLearnerJobOutput.class);
    }
}
