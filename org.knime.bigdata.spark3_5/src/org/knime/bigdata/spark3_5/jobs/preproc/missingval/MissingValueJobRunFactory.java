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
 */
package org.knime.bigdata.spark3_5.jobs.preproc.missingval;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobInput;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueJobOutput;
import org.knime.bigdata.spark.node.preproc.missingval.compute.SparkMissingValueNodeModel;

/**
 * Missing value job factory.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class MissingValueJobRunFactory
    extends DefaultJobRunFactory<SparkMissingValueJobInput, SparkMissingValueJobOutput> {

    /** Default Constructor. */
    public MissingValueJobRunFactory() {
        super(SparkMissingValueNodeModel.JOB_ID, MissingValueJob.class, SparkMissingValueJobOutput.class);
    }
}
