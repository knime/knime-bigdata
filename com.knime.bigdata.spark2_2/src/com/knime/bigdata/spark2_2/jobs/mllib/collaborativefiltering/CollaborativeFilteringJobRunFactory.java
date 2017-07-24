/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on 01.05.2016 by koetter
 */
package com.knime.bigdata.spark2_2.jobs.mllib.collaborativefiltering;

import com.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import com.knime.bigdata.spark.node.mllib.collaborativefiltering.CollaborativeFilteringJobInput;
import com.knime.bigdata.spark.node.mllib.collaborativefiltering.CollaborativeFilteringJobOutput;
import com.knime.bigdata.spark.node.mllib.collaborativefiltering.MLlibCollaborativeFilteringNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class CollaborativeFilteringJobRunFactory
    extends DefaultJobRunFactory<CollaborativeFilteringJobInput, CollaborativeFilteringJobOutput> {

    /** Constructor. */
    public CollaborativeFilteringJobRunFactory() {
        super(MLlibCollaborativeFilteringNodeModel.JOB_ID, CollaborativeFilteringJob.class,
            CollaborativeFilteringJobOutput.class);
    }
}
