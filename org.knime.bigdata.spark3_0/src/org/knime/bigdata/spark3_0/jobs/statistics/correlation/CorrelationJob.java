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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark3_0.jobs.statistics.correlation;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.statistics.correlation.CorrelationJobInput;
import org.knime.bigdata.spark3_0.api.NamedObjects;
import org.knime.bigdata.spark3_0.api.RDDUtilsInJava;
import org.knime.bigdata.spark3_0.api.SparkJob;

/**
 * Computes correlation
 *
 * @author Tobias Koetter, KNIME.com, dwk
 * @param <O> the {@link JobOutput} implementation
 */
@SparkClass
public abstract class CorrelationJob<O extends JobOutput> implements SparkJob<CorrelationJobInput, O> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(CorrelationJob.class.getName());

    @Override
    public O runJob(final SparkContext sparkContext, final CorrelationJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {

        LOGGER.info("starting Correlation Computation job...");
        final Dataset<Row> dataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final JavaRDD<Vector> data = RDDUtilsInJava.toVectorRdd(dataset, input.getColumnIdxs());
        final Matrix mat = Statistics.corr(data.rdd(), input.getMethod().toString().toLowerCase());
        final O output = createJobOutput(mat);
        if (input.hasFirstNamedOutputObject()) {
            final Dataset<Row> outputDataset =
                    RDDUtilsInJava.fromMatrix(sparkContext, mat, "dummy")
                    .toDF(input.getColumnNames(dataset.schema().fieldNames()));
            namedObjects.addDataFrame(input.getFirstNamedOutputObject(), outputDataset);
        }

        LOGGER.info("Correlation Computation done");
        return output;
    }

    /**
     * @param mat the computed correlation {@link Matrix}
     * @return the {@link JobOutput}
     */
    protected abstract O createJobOutput(final Matrix mat);
}
