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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * runs MLlib KMeans on a given RDD, model is returned as result
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
public class KMeansLearner extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * number of clusters for cluster learners
     */
    public static final String PARAM_NUM_CLUSTERS = "noOfClusters";

    private static final String PARAM_NUM_ITERATIONS = ParameterConstants.PARAM_NUM_ITERATIONS;

    private final static Logger LOGGER = Logger.getLogger(KMeansLearner.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;
        if (!aConfig.hasInputParameter(PARAM_NUM_CLUSTERS)) {
            msg = "Input parameter '" + PARAM_NUM_CLUSTERS + "' missing.";
        } else {
            try {
                final int ix = aConfig.getInputParameter(PARAM_NUM_CLUSTERS, Integer.class);
                if (ix < 1) {
                    msg = "Input parameter '" + PARAM_NUM_CLUSTERS + "' must be a positive number.";
                }
            } catch (Exception e) {
                msg = "Input parameter '" + PARAM_NUM_CLUSTERS + "' is not of expected type 'integer'.";
            }
        }
        if (msg == null) {
            if (!aConfig.hasInputParameter(PARAM_NUM_ITERATIONS)) {
                msg = "Input parameter '" + PARAM_NUM_ITERATIONS + "' missing.";
            } else {
                try {
                    final int ix = aConfig.getInputParameter(PARAM_NUM_ITERATIONS, Integer.class);
                    if (ix < 1) {
                        msg = "Input parameter '" + PARAM_NUM_ITERATIONS + "' must be a positive number.";
                    }
                } catch (Exception e) {
                    msg = "Input parameter '" + PARAM_NUM_ITERATIONS + "' is not of expected type 'integer'.";
                }
            }
        }

        if (msg == null) {
            msg = SupervisedLearnerUtils.checkSelectedColumnIdsParameter(aConfig);
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + PARAM_INPUT_TABLE + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    private void validateInput(final JobConfig aConfig) throws GenericKnimeSparkException {
        String msg = null;
        final String key = aConfig.getInputParameter(PARAM_INPUT_TABLE);
        if (key == null) {
            msg = "Input parameter at port 1 is missing!";
        } else if (!validateNamedRdd(key)) {
            msg = "Input data table missing!";
        }
        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
        }
    }

    /**
     * run the actual job, the result is serialized back to the client
     *
     * @throws GenericKnimeSparkException
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        validateInput(aConfig);
        LOGGER.log(Level.INFO, "starting kMeans job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        final List<Integer> colIdxs = SupervisedLearnerUtils.getSelectedColumnIds(aConfig);

        //use only the column indices when converting to vector
        final JavaRDD<Vector> inputRDD = RDDUtils.toJavaRDDOfVectorsOfSelectedIndices(rowRDD, colIdxs);

        final KMeansModel model = execute(sc, aConfig, inputRDD);

        JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(model);

        SupervisedLearnerUtils.storePredictions(sc, aConfig, this, rowRDD, inputRDD, model, LOGGER);

        LOGGER.log(Level.INFO, "kMeans done");
        // note that with Spark 1.4 we can use PMML instead
        return res;
    }

    private KMeansModel execute(final SparkContext aContext, final JobConfig aConfig, final JavaRDD<Vector> aInputData) {
        aInputData.cache();

        final int noOfCluster = aConfig.getInputParameter(PARAM_NUM_CLUSTERS, Integer.class);
        final int noOfIteration = aConfig.getInputParameter(PARAM_NUM_ITERATIONS, Integer.class);

        // Cluster the data into m_noOfCluster classes using KMeans
        return KMeans.train(aInputData.rdd(), noOfCluster, noOfIteration);
    }

}
