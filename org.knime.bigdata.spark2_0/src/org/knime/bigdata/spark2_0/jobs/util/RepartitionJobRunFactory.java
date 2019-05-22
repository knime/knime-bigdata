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
 */
package org.knime.bigdata.spark2_0.jobs.util;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.node.util.repartition.RepartitionJobInput;
import org.knime.bigdata.spark.node.util.repartition.RepartitionJobOutput;
import org.knime.bigdata.spark.node.util.repartition.SparkRepartitionNodeModel;

/**
 * Spark repartition job factory.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class RepartitionJobRunFactory extends DefaultJobRunFactory<RepartitionJobInput, RepartitionJobOutput> {

    /**
     * Default constructor.
     */
    public RepartitionJobRunFactory() {
        super(SparkRepartitionNodeModel.JOB_ID, RepartitionJob.class, RepartitionJobOutput.class);
    }
}
