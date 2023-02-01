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
 *   Created on 29.04.2016 by koetter
 */
package org.knime.bigdata.spark3_3.jobs.preproc.normalize;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.node.preproc.normalize.NormalizeJobInput;
import org.knime.bigdata.spark.node.preproc.normalize.NormalizeJobOutput;
import org.knime.bigdata.spark.node.preproc.normalize.SparkNormalizerPMMLNodeModel;

/**
 *
 * @author Ole Ostergaard
 */
public class NormalizeColumnsJobRunFactory extends DefaultJobRunFactory<NormalizeJobInput, NormalizeJobOutput> {

    /**
     * Constructor.
     */
    public NormalizeColumnsJobRunFactory() {
        super(SparkNormalizerPMMLNodeModel.JOB_ID, NormalizeColumnsJob.class, NormalizeJobOutput.class);
    }
}
