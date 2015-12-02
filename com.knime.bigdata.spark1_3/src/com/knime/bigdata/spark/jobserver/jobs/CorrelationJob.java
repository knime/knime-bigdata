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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.jobserver.server.EnumContainer.CorrelationMethods;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.HalfDoubleMatrixFromLinAlgMatrix;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

import spark.jobserver.SparkJobValidation;

/**
 * computes correlation of selected indices
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
public class CorrelationJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(CorrelationJob.class.getName());

    /**
     * either pearson or spearman
     */
    public static final String PARAM_STAT_METHOD = "statMethod";

    /**
     * indicates whether correlations should be returned as a matrix
     */
    public static final String PARAM_RETURN_MATRIX = "returnMatrix";

    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = SupervisedLearnerUtils.checkSelectedColumnIdsParameter(aConfig);

        if (msg == null && !aConfig.hasInputParameter(PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + PARAM_INPUT_TABLE + "' missing.";
        }

        if (!aConfig.hasInputParameter(PARAM_STAT_METHOD)) {
            msg = "Input parameter '" + PARAM_STAT_METHOD + "' missing.";
        }

        if (msg == null && !aConfig.hasOutputParameter(PARAM_RESULT_TABLE)
            && SupervisedLearnerUtils.getSelectedColumnIds(aConfig).size() > 2) {
            msg = "Output parameter '" + PARAM_RESULT_TABLE + "' missing.";
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

    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        validateInput(aConfig);
        LOGGER.log(Level.INFO, "starting Correlation Computation job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        final List<Integer> colIdxs = SupervisedLearnerUtils.getSelectedColumnIds(aConfig);

        final Matrix mat =
            computeCorrelation(rowRDD, colIdxs, CorrelationMethods.fromKnimeEnum(aConfig.getInputParameter(PARAM_STAT_METHOD)));

        final Serializable correlation;
        if (aConfig.hasOutputParameter(PARAM_RETURN_MATRIX)) {
            correlation = new HalfDoubleMatrixFromLinAlgMatrix(mat, false);
        }
        else if (!aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            correlation = mat.apply(0, 1);
        } else {
            correlation = Double.MIN_VALUE;
        }

        if (aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE),
                RDDUtilsInJava.fromMatrix(JavaSparkContext.fromSparkContext(sc), mat));
        }

        LOGGER.log(Level.INFO, "Correlation Computation done");
        return JobResult.emptyJobResult().withObjectResult(correlation).withMessage("OK");
    }

    static Matrix computeCorrelation(final JavaRDD<Row> aRowRDD, final List<Integer> aColIdxs, final CorrelationMethods aCorrelationMethod) {
        final JavaRDD<Vector> data = RDDUtils.toJavaRDDOfVectorsOfSelectedIndices(aRowRDD, aColIdxs);
        return Statistics.corr(data.rdd(), aCorrelationMethod.toString());
    }
}
