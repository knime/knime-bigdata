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
package com.knime.bigdata.spark1_5.jobs.preproc.convert.number2category;

import com.knime.bigdata.spark.core.job.DefaultSimpleJobRun;
import com.knime.bigdata.spark.core.job.DefaultSimpleJobRunFactory;
import com.knime.bigdata.spark.core.job.SimpleJobRun;
import com.knime.bigdata.spark.node.preproc.convert.number2category.Number2CategoryJobInput;
import com.knime.bigdata.spark.node.preproc.convert.number2category.SparkNumber2CategoryNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Number2CategoryJobRunFactory extends DefaultSimpleJobRunFactory<Number2CategoryJobInput> {

    /**
     *
     */
    public Number2CategoryJobRunFactory() {
        super(SparkNumber2CategoryNodeModel.JOB_ID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SimpleJobRun<Number2CategoryJobInput> createRun(final Number2CategoryJobInput input) {
        return new DefaultSimpleJobRun<>(input, Number2CategoryJob.class);
    }
}
