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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.jobserver.server.EntropyScorerData;
import com.knime.bigdata.spark.jobserver.server.EntropyScorerData.ClusterScore;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

import scala.Tuple2;
import spark.jobserver.SparkJobValidation;

/**
 * computes cluster some entropy and quality values for clustering results given a reference clustering.
 *
 * @author dwk
 * @author Bjoern Lohrmann, KNIME.com
 */
public class EntropyScorerJob extends KnimeSparkJob implements Serializable {

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

    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;
        if (!aConfig.hasInputParameter(PARAM_ACTUAL_COL_INDEX)) {
            msg = "Input parameter '" + PARAM_ACTUAL_COL_INDEX + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_PREDICTION_COL_INDEX)) {
            msg = "Input parameter '" + PARAM_PREDICTION_COL_INDEX + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(KnimeSparkJob.PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + KnimeSparkJob.PARAM_INPUT_TABLE + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }

        return ValidationResultConverter.valid();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JobResult runJobWithContext(final SparkContext aSparkContext, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        SupervisedLearnerUtils.validateInput(aConfig, this, getLogger());
        getLogger().log(Level.INFO, "START " + getAlgName() + " job...");

        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));

        final Integer referenceCol = aConfig.getInputParameter(PARAM_ACTUAL_COL_INDEX, Integer.class);
        final Integer clusterCol = aConfig.getInputParameter(PARAM_PREDICTION_COL_INDEX, Integer.class);
        final Serializable scores = scoreCluster(rowRDD, referenceCol, clusterCol);

        JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(scores);

        getLogger().log(Level.INFO, "DONE " + getAlgName() + " job...");
        return res;
    }

    private static Serializable scoreCluster(final JavaRDD<Row> aRowRDD, final Integer referenceCol,
        final Integer clusterCol) {
        // maps cluster -> (refCluster -> count)
        final Map<Object, Map<Object, Integer>> counts =
            toMapOfMaps(RDDUtilsInJava.aggregatePairs(aRowRDD, clusterCol, referenceCol));

        final int nrReferenceClusters = computeNrReferenceClusters(counts);
        final List<ClusterScore> clusterScores = computeClusterScores(counts, nrReferenceClusters);

        return computeOverallValuesAndCreateEntropyScorerData(clusterScores, nrReferenceClusters);
    }

    private static EntropyScorerData computeOverallValuesAndCreateEntropyScorerData(
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

        return new EntropyScorerData(clusterScores, overallEntropy, overallNormalizedEntropy, overallQuality,
            overallSize, nrClusters, nrReferenceClusters);
    }

    private static int computeNrReferenceClusters(final Map<Object, Map<Object, Integer>> counts) {
        // get the number of different clusters in the reference set
        final Set<Object> reference = new HashSet<>();
        for (Map<Object, Integer> clusterCounts : counts.values()) {
            reference.addAll(clusterCounts.keySet());
        }
        return reference.size();
    }

    private static Map<Object, Map<Object, Integer>>
        toMapOfMaps(final Map<Tuple2<Object, Object>, Integer> mapOfTuples) {

        final Map<Object, Map<Object, Integer>> mapOfMaps = new HashMap<>();
        for (Tuple2<Object, Object> tuple : mapOfTuples.keySet()) {
            Map<Object, Integer> map = mapOfMaps.get(tuple._1);
            if (map == null) {
                map = new HashMap<>();
                mapOfMaps.put(tuple._1, map);
            }

            Integer count = map.get(tuple._2);
            if (count == null) {
                count = 0;
            }
            map.put(tuple._2, count + mapOfTuples.get(tuple));
        }

        return mapOfMaps;
    }

    private static List<ClusterScore> computeClusterScores(final Map<Object, Map<Object, Integer>> counts,
        final int nrReferenceClusters) {

        final List<ClusterScore> clusterScores = new LinkedList<>();

        // normalizing value (such that the maximum value for the entropy is 1
        final double normalizer = Math.log(nrReferenceClusters) / Math.log(2d);

        for (Object cluster : counts.keySet()) {
            Map<Object, Integer> clusterCounts = counts.get(cluster);

            final int clusterSize = computeClusterSize(clusterCounts);
            final double clusterEntropy = clusterEntropy(clusterCounts, clusterSize);
            final double normalizedClusterEntropy = clusterEntropy / normalizer;
            clusterScores.add(new ClusterScore(cluster, clusterSize, clusterEntropy, normalizedClusterEntropy));
        }
        return clusterScores;
    }

    /**
     * Get entropy for one single cluster.
     *
     * @param cluster the single cluster to score
     * @return the (not-normalized) entropy of <code>pats</code> wrt. <code>ref</code>
     */
    static double clusterEntropy(final Map<Object, Integer> cluster, final int size) {

        double e = 0.0;
        for (int clusterCount : cluster.values()) {
            double quot = clusterCount / (double)size;
            e -= quot * Math.log(quot) / Math.log(2.0);
        }
        return e;
    }

    /**
     * Counts the number of objects that were classified as within the same cluster.
     *
     * @param map from _reference_ cluster to number of objects
     * @return number of objects in cluster
     */
    private static int computeClusterSize(final Map<Object, Integer> cluster) {
        int size = 0;
        for (Integer s : cluster.values()) {
            size += s;
        }
        return size;
    }
}
