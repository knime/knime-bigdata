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
package org.knime.bigdata.spark1_2.jobs.statistics.compute;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.api.java.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.ColumnsJobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.statistics.compute.StatisticsJobOutput;
import org.knime.bigdata.spark1_2.api.NamedObjects;
import org.knime.bigdata.spark1_2.api.SparkJob;

import com.knime.bigdata.spark.jobserver.server.RDDUtils;

/**
 * computes multivariate statistics from input RDD and given indices
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
@SparkClass
public class StatisticsJob implements SparkJob<ColumnsJobInput, StatisticsJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(StatisticsJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public StatisticsJobOutput runJob(final SparkContext sparkContext, final ColumnsJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {
        LOGGER.info("starting Multivariate Statistics job...");
        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());
        final List<Integer> colIdxs = input.getColumnIdxs();
        final JavaRDD<Vector> data = RDDUtils.toJavaRDDOfVectorsOfSelectedIndices(rowRDD, colIdxs);
        MultivariateStatisticalSummary stats = Statistics.colStats(data.rdd());
        LOGGER.log(Level.INFO, "Multivariate Statistics done");
        return new StatisticsJobOutput(stats.count(), stats.min().toArray(), stats.max().toArray(),
            stats.mean().toArray(), stats.variance().toArray(), stats.normL1().toArray(), stats.normL2().toArray(),
            stats.numNonzeros().toArray());
    }
}
