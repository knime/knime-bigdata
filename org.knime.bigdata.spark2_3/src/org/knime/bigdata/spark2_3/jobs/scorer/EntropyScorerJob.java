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
package org.knime.bigdata.spark2_3.jobs.scorer;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.sum;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.scorer.ScorerJobInput;
import org.knime.bigdata.spark.node.scorer.entropy.EntropyScorerData.ClusterScore;
import org.knime.bigdata.spark.node.scorer.entropy.EntropyScorerJobOutput;
import org.knime.bigdata.spark2_3.api.NamedObjects;
import org.knime.bigdata.spark2_3.api.SparkJob;

/**
 * computes cluster some entropy and quality values for clustering results given a reference clustering.
 *
 * @author dwk
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class EntropyScorerJob implements SparkJob<ScorerJobInput, EntropyScorerJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(EntropyScorerJob.class.getName());

    /**
     * index of column with actual (true) class or regression values
     */
    public static final String PARAM_ACTUAL_COL_INDEX = "actualColIndex";

    /**
     * index of column with predicted class or regression values
     */
    public static final String PARAM_PREDICTION_COL_INDEX = "predictionColIndex";

    Logger getLogger() {
        return LOGGER;
    }

    String getAlgName() {
        return "EntropyScorer";
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public EntropyScorerJobOutput  runJob(final SparkContext sparkContext, final ScorerJobInput input, final NamedObjects namedObjects) {
        getLogger().info("START " + getAlgName() + " job...");

        final Dataset<Row> dataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());

        final String referenceCol = dataset.columns()[input.getRefColIdx()];
        final String clusterCol = dataset.columns()[input.getPredictionColIdx()];
        EntropyScorerJobOutput jobOutput = scoreCluster(dataset, referenceCol, clusterCol);

        getLogger().info("DONE " + getAlgName() + " job...");
        return jobOutput;
    }

    private static EntropyScorerJobOutput scoreCluster(final Dataset<Row> dataset, final String referenceCol,
        final String clusterCol) {

        final Dataset<Row> counts = dataset.groupBy(col(clusterCol).as("cluster"), col(referenceCol).as("ref")).count();
        final long nrReferenceClusters = counts.select("ref").distinct().count();
        final double normalizer = Math.log(nrReferenceClusters) / Math.log(2d);

        final Dataset<Row> clusters = counts.groupBy("cluster")
            .agg(collect_list("count").as("counts"), sum("count").as("clusterSize"));

        final List<ClusterScore> clusterScores = clusters.javaRDD().map(new Function<Row, ClusterScore>() {
            private static final long serialVersionUID = 1L;

            private double clusterEntropy(final List<Long> clusterCounts, final long clusterSize) {
                double e = 0.0;
                for (final long count : clusterCounts) {
                    double quot = count / (double)clusterSize;
                    e -= quot * Math.log(quot) / Math.log(2.0);
                }
                return e;
            }

            @Override
            public ClusterScore call(final Row row) throws Exception {
                // row: cluster, counts, clusterSize
                final Object cluster = row.get(0);
                final long clusterSize = row.getLong(2);
                final double clusterEntropy = clusterEntropy(row.getList(1), clusterSize);
                final double normalizedClusterEntropy = clusterEntropy / normalizer;
                return new ClusterScore(cluster, (int) clusterSize, clusterEntropy, normalizedClusterEntropy);
            }
        }).collect();

        return computeOverallValuesAndCreateEntropyScorerData(clusterScores, (int) nrReferenceClusters);
    }

    private static EntropyScorerJobOutput computeOverallValuesAndCreateEntropyScorerData(
        final List<ClusterScore> clusterScores, final int nrReferenceClusters) {

        final int nrClusters = clusterScores.size();

        int overallSize = 0;
        double overallEntropy = 0;
        double overallNormalizedEntropy = 0;
        double overallQuality = 0;

        if (clusterScores.isEmpty()) {
            overallEntropy = 0;
            overallNormalizedEntropy = 0;
            // optimistic guess (we don't have counterexamples!)
            overallQuality = 1;
        } else {
            for (ClusterScore clusterScore : clusterScores) {
                overallSize += clusterScore.getSize();
                overallEntropy += clusterScore.getSize() * clusterScore.getEntropy();
                overallNormalizedEntropy += clusterScore.getSize() * clusterScore.getNormalizedEntropy();
                overallQuality += clusterScore.getSize() * (1.0 - clusterScore.getNormalizedEntropy());
            }

            overallQuality /= overallSize;
            overallEntropy /= overallSize;
            overallNormalizedEntropy /= overallSize;
        }

        return new EntropyScorerJobOutput(clusterScores, overallEntropy, overallNormalizedEntropy, overallQuality,
            overallSize, nrClusters, nrReferenceClusters, 0 /* TODO missing values */);
    }

}
