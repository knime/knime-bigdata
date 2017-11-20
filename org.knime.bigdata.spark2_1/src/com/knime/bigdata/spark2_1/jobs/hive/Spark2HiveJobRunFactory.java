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
package com.knime.bigdata.spark2_1.jobs.hive;

import com.knime.bigdata.spark.core.job.DefaultSimpleJobRunFactory;
import com.knime.bigdata.spark.node.io.hive.writer.Spark2HiveJobInput;
import com.knime.bigdata.spark.node.io.hive.writer.Spark2HiveNodeModel;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark2HiveJobRunFactory extends DefaultSimpleJobRunFactory<Spark2HiveJobInput> {

    /**
     * Constructor.
     */
    public Spark2HiveJobRunFactory() {
        super(Spark2HiveNodeModel.JOB_ID, Spark2HiveJob.class);
    }
}
