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
 *   Created on Apr 27, 2016 by bjoern
 */
package org.knime.bigdata.spark1_3.jobs.prepare;

import org.knime.bigdata.spark.core.context.SparkContextConstants;
import org.knime.bigdata.spark.core.context.util.PrepareContextJobInput;
import org.knime.bigdata.spark.core.job.DefaultSimpleJobRunFactory;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class PrepareContextJobRunFactory extends DefaultSimpleJobRunFactory<PrepareContextJobInput> {

    /**
     * Constructor.
     */
    public PrepareContextJobRunFactory() {
        super(SparkContextConstants.PREPARE_CONTEXT_JOB_ID, PrepareContextJob.class);
    }
}
