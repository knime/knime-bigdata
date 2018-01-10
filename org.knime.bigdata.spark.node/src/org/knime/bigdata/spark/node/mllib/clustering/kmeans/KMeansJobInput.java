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
package org.knime.bigdata.spark.node.mllib.clustering.kmeans;

import java.util.List;

import org.knime.bigdata.spark.core.job.ColumnsJobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class KMeansJobInput extends ColumnsJobInput {

    private final static String KEY_NO_OF_ITERATIONS = "noOfIterations";

    private final static String KEY_NO_CLUSTERS = "noOfClusters";

    private final static String KEY_SEED = "seed";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public KMeansJobInput() {
    }

    /**
     * Creates a KMeans input with the given parameters.
     *
     * @param namedInputObject the unique id of the input SparkRDD
     * @param featureColumnIdxs - indices of the columns to include starting with 0
     * @param noOfClusters - number of clusters (aka "k")
     * @param noOfIterations - maximal number of iterations
     * @param namedOutputObject - table identifier (classified output data)
     * @param outputSpec - output intermediate specification
     * @param seed - random seed for cluster initialization
     */
    public KMeansJobInput(final String namedInputObject, final String namedOutputObject,
        final IntermediateSpec outputSpec, final List<Integer> featureColumnIdxs, final int noOfClusters,
        final int noOfIterations, final long seed) {

        super(namedInputObject, featureColumnIdxs);
        if (noOfClusters < 1) {
            throw new IllegalArgumentException("Number of clusters must not be smaller than 1.");
        }

        if (noOfIterations < 1) {
            throw new IllegalArgumentException("Number of iterations must not be smaller than 1.");
        }
        addNamedOutputObject(namedOutputObject);
        withSpec(namedOutputObject, outputSpec);
        set(KEY_NO_CLUSTERS, noOfClusters);
        set(KEY_NO_OF_ITERATIONS, noOfIterations);
        set(KEY_SEED, seed);
    }

    /**
     * @return the name of the result/cluster column (last column in spec)
     */
    public String getPredictionColumnName() {
        final IntermediateField fields[] = getSpec(getFirstNamedOutputObject()).getFields();
        return fields[fields.length - 1].getName();
    }

    /**
     * @return the number of clusters
     */
    public int getNoOfClusters() {
        return getInteger(KEY_NO_CLUSTERS);
    }

    /**
     * @return the number of iterations
     */
    public int getNoOfIterations() {
        return getInteger(KEY_NO_OF_ITERATIONS);
    }

    /** @return seed parameter */
    public long getSeed() {
        return getLong(KEY_SEED);
    }
}
