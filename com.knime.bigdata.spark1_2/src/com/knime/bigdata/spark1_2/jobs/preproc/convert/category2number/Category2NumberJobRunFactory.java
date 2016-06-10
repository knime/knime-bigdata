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
 *   Created on 29.04.2016 by koetter
 */
package com.knime.bigdata.spark1_2.jobs.preproc.convert.category2number;

import com.knime.bigdata.spark.core.job.DefaultJobRun;
import com.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import com.knime.bigdata.spark.core.job.JobRun;
import com.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobInput;
import com.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberJobOutput;
import com.knime.bigdata.spark.node.preproc.convert.category2number.SparkCategory2NumberNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Category2NumberJobRunFactory extends DefaultJobRunFactory<Category2NumberJobInput, Category2NumberJobOutput> {

    /**
     *
     */
    public Category2NumberJobRunFactory() {
        super(SparkCategory2NumberNodeModel.JOB_ID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobRun<Category2NumberJobInput, Category2NumberJobOutput> createRun(final Category2NumberJobInput input) {
        return new DefaultJobRun<>(input, Category2NumberJob.class, Category2NumberJobOutput.class);
    }
}
