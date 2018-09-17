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
 *   Created on 24.08.2018 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark2_3.jobs.scripting.python;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.scripting.python.PySparkNodeModel;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkJobInput;
import org.knime.bigdata.spark.node.scripting.python.util.PySparkJobOutput;

/**
 * Factory for PySpark Job
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
@SparkClass
public class PySparkJobRunFactory extends DefaultJobRunFactory<PySparkJobInput, PySparkJobOutput> {

    /** Default constructor. */
    public PySparkJobRunFactory() {
        super(PySparkNodeModel.JOB_ID, PySparkJob.class, PySparkJobOutput.class);
    }

}
