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
package org.knime.bigdata.spark2_1.jobs.statistics.correlation;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.statistics.correlation.CorrelationJobInput;
import org.knime.bigdata.spark2_1.api.NamedObjects;
import org.knime.bigdata.spark2_1.api.RDDUtilsInJava;
import org.knime.bigdata.spark2_1.api.SparkJob;

import com.knime.bigdata.spark.jobserver.server.RDDUtils;

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
        final JavaRDD<Row> rowRDD = namedObjects.getDataFrame(input.getFirstNamedInputObject()).javaRDD();
        final List<Integer> colIdxs = input.getColumnIdxs();
        final JavaRDD<Vector> data = RDDUtils.toJavaRDDOfVectorsOfSelectedIndices(rowRDD, colIdxs);
        final Matrix mat = Statistics.corr(data.rdd(), input.getMethod().toString().toLowerCase());
        final O output = createJobOutput(mat);
        if (input.hasFirstNamedOutputObject()) {
            final Dataset<Row> outputRdd = RDDUtilsInJava.fromMatrix(JavaSparkContext.fromSparkContext(sparkContext), mat);
            namedObjects.addDataFrame(input.getFirstNamedOutputObject(), outputRdd);
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
