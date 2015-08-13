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
 *   Created on 12.08.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 *
 * @author dwk
 */
public class PCAJob extends KnimeSparkJob {

    private final static Logger LOGGER = Logger.getLogger(PCAJob.class.getName());

    /**
     * number of top principal components.
     */
    public static final String PARAM_K = ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_STRING, 2);

    /**
     * named RDD with principal components
     */
    public static final String PARAM_RESULT_MATRIX = ParameterConstants.NUMBERED_PARAM(
        ParameterConstants.PARAM_STRING, 1);

    /**
     * parse parameters
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;
        {
            if (!aConfig.hasInputParameter(SupervisedLearnerUtils.PARAM_TRAINING_RDD)) {
                msg = "Input parameter '" + SupervisedLearnerUtils.PARAM_TRAINING_RDD + "' missing.";
            }
        }

        if (msg == null) {
            if (!aConfig.hasInputParameter(PARAM_K)) {
                msg = "Input parameter '" + PARAM_K + "' missing.";
            } else {
                try {
                    getK(aConfig);
                } catch (Exception e) {
                    msg = "Input parameter '" + PARAM_K + "' is not of expected type 'integer'.";
                }
            }
        }

        if (msg == null) {
            msg = SupervisedLearnerUtils.checkSelectedColumnIdsParameter(aConfig);
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    static Integer getK(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_K, Integer.class);
    }

    /**
     * run the actual job, the result is serialized back to the client
     *
     * @throws GenericKnimeSparkException
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        SupervisedLearnerUtils.validateInput(aConfig, this, LOGGER);
        LOGGER.log(Level.INFO, "starting PCA job...");

        final JavaRDD<Row> rowRDD =
            getFromNamedRdds(aConfig.getInputParameter(SupervisedLearnerUtils.PARAM_TRAINING_RDD));

        Matrix pca = compute(aConfig, rowRDD);

        JobResult res =
            JobResult.emptyJobResult().withMessage("OK").withObjectResult(pca.toArray());

        if (aConfig.hasOutputParameter(PARAM_RESULT_MATRIX)) {
            final JavaSparkContext js = JavaSparkContext.fromSparkContext(sc);
            addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_MATRIX), RDDUtilsInJava.fromMatrix(js, pca));
        }

        LOGGER.log(Level.INFO, "PCA done");

        return res;
    }

    /**
     * @param aConfig
     * @param aRowRDD
     * @return
     */
    static Matrix compute(final JobConfig aConfig,
        final JavaRDD<Row> aRowRDD) {
        // Create a RowMatrix from JavaRDD<Row>.
        RowMatrix mat = RDDUtilsInJava.toRowMatrix(aRowRDD, SupervisedLearnerUtils.getSelectedColumnIds(aConfig));

        final int k = getK(aConfig);

        // Compute the top k singular values and corresponding singular vectors.
        return mat.computePrincipalComponents(k);
    }

}
