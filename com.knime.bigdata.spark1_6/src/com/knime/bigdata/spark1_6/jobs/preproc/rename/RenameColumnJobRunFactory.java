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
 *   Created on Jan 17, 2017 by Sascha Wolke, KNIME.com
 */
package com.knime.bigdata.spark1_6.jobs.preproc.rename;

import com.knime.bigdata.spark.core.job.DefaultSimpleJobRun;
import com.knime.bigdata.spark.core.job.DefaultSimpleJobRunFactory;
import com.knime.bigdata.spark.core.job.SimpleJobRun;
import com.knime.bigdata.spark.node.preproc.rename.RenameColumnJobInput;
import com.knime.bigdata.spark.node.preproc.rename.SparkRenameColumnNodeModel;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class RenameColumnJobRunFactory extends DefaultSimpleJobRunFactory<RenameColumnJobInput> {

    /** Default constructor. */
    public RenameColumnJobRunFactory() {
        super(SparkRenameColumnNodeModel.JOB_ID);
    }

    @Override
    public SimpleJobRun<RenameColumnJobInput> createRun(final RenameColumnJobInput input) {
        return new DefaultSimpleJobRun<>(input, RenameColumnJob.class);
    }
}
