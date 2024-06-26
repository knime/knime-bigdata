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
package org.knime.bigdata.spark3_5.jobs.mllib.freqitemset;

import org.knime.bigdata.spark.core.job.DefaultSimpleJobRunFactory;
import org.knime.bigdata.spark.node.mllib.freqitemset.FrequentItemSetJobInput;
import org.knime.bigdata.spark.node.mllib.freqitemset.SparkFrequentItemSetNodeModel;

/**
 * Job run factory of frequent item sets job.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class FrequentItemSetJobRunFactory extends DefaultSimpleJobRunFactory<FrequentItemSetJobInput> {

    /** Default constructor. */
    public FrequentItemSetJobRunFactory() {
        super(SparkFrequentItemSetNodeModel.JOB_ID, FrequentItemSetJob.class);
    }
}
