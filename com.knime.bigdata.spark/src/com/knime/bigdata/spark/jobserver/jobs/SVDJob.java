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
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
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
public class SVDJob extends KnimeSparkJob {

    private final static Logger LOGGER = Logger.getLogger(SVDJob.class.getName());

    /**
     * the reciprocal condition number. All singular values smaller than rCond * sigma(0) are treated as zero, where
     * sigma(0) is the largest singular value.
     */
    public static final String PARAM_RCOND = ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_STRING, 1);

    /**
     * number of leading singular values to keep (0 < k <= n). It might return less than k if there are numerically zero
     * singular values or there are not enough Ritz values converged before the maximum number of Arnoldi update
     * iterations is reached (in case that matrix A is ill-conditioned).
     */
    public static final String PARAM_K = ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_STRING, 2);

    /**
     * whether to compute U
     */
    public static final String PARAM_COMPUTE_U = ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_STRING, 3);

    /**
     * named RDD with V matrix
     */
    public static final String PARAM_RESULT_MATRIX_V = ParameterConstants.NUMBERED_PARAM(
        ParameterConstants.PARAM_STRING, 1);

    /**
     * named RDD with U matrix (iff computeU == true)
     */
    public static final String PARAM_RESULT_MATRIX_U = ParameterConstants.NUMBERED_PARAM(
        ParameterConstants.PARAM_STRING, 2);

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
            if (!aConfig.hasInputParameter(PARAM_RCOND)) {
                msg = "Input parameter '" + PARAM_RCOND + "' missing.";
            } else {
                try {
                    getRCond(aConfig);
                } catch (Exception e) {
                    msg = "Input parameter '" + PARAM_RCOND + "' is not of expected type 'double'.";
                }
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
            if (!aConfig.hasInputParameter(PARAM_COMPUTE_U)) {
                msg = "Input parameter '" + PARAM_COMPUTE_U + "' missing.";
            } else {
                try {
                    getComputeU(aConfig);
                } catch (Exception e) {
                    msg = "Input parameter '" + PARAM_COMPUTE_U + "' is not of expected type 'boolean'.";
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

    /**
     * @param aConfig
     */
    static Double getRCond(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_RCOND, Double.class);
    }

    static Integer getK(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_K, Integer.class);
    }

    static Boolean getComputeU(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_COMPUTE_U, Boolean.class);
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
        LOGGER.log(Level.INFO, "starting SVD job...");

        final JavaRDD<Row> rowRDD =
            getFromNamedRdds(aConfig.getInputParameter(SupervisedLearnerUtils.PARAM_TRAINING_RDD));

        SingularValueDecomposition<RowMatrix, Matrix> svd = decomposeRowRdd(aConfig, rowRDD);


        JobResult res =
            JobResult.emptyJobResult().withMessage("OK").withObjectResult(svd.s().toArray());


        if (aConfig.hasOutputParameter(PARAM_RESULT_MATRIX_V)) {
            final JavaSparkContext js = JavaSparkContext.fromSparkContext(sc);
            final JavaRDD<Row> V = RDDUtilsInJava.fromMatrix(js, svd.V());
            addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_MATRIX_V), V);
        }

        if (getComputeU(aConfig) && aConfig.hasOutputParameter(PARAM_RESULT_MATRIX_U)) {
            final JavaRDD<Row> U = RDDUtilsInJava.fromRowMatrix(svd.U());
            addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_MATRIX_U), U);
        }

        //this would be an alternative to converting the matrices:
//        List<Object> tmp = new ArrayList<>();
//        tmp.add(svd);
//        JavaSparkContext js = JavaSparkContext.fromSparkContext(sc);
//        JavaRDD<Object> tmpRdd = js.parallelize(tmp);
//        addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_MATRIX_U), tmpRdd);
//
//        JavaRDD<Object> named = getFromNamedRddsAsObject(aConfig.getOutputStringParameter(PARAM_RESULT_MATRIX_U));
//        List<Object> l = named.take(1);
//        SingularValueDecomposition<RowMatrix, Matrix> svd2 = (SingularValueDecomposition<RowMatrix, Matrix>)l.get(0);

        LOGGER.log(Level.INFO, "SVD done");

        return res;
    }

    /**
     * @param aConfig
     * @param aRowRDD
     * @return
     */
    static SingularValueDecomposition<RowMatrix, Matrix> decomposeRowRdd(final JobConfig aConfig,
        final JavaRDD<Row> aRowRDD) {
        // Create a RowMatrix from JavaRDD<Row>.
        RowMatrix mat = RDDUtilsInJava.toRowMatrix(aRowRDD, SupervisedLearnerUtils.getSelectedColumnIds(aConfig));

        final int k = getK(aConfig);
        final double rCond = getRCond(aConfig);

        // Compute the top k singular values and corresponding singular vectors.
        return mat.computeSVD(k, getComputeU(aConfig), rCond);
    }

}
