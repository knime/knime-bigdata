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
 */
package org.knime.bigdata.spark1_6.jobs.mllib.reduction.pca;


import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.reduction.pca.PCAJobInput;
import org.knime.bigdata.spark.node.mllib.reduction.pca.PCAJobOutput;
import org.knime.bigdata.spark1_6.api.NamedObjects;
import org.knime.bigdata.spark1_6.api.RDDUtilsInJava;
import org.knime.bigdata.spark1_6.api.SparkJob;

import breeze.linalg.NotConvergedException;

/**
 * Spark PCA job.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class PCAJob implements SparkJob<PCAJobInput, PCAJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(PCAJob.class.getName());

    @Override
    public PCAJobOutput runJob(final SparkContext sparkContext, final PCAJobInput input, final NamedObjects namedObjects)
        throws Exception {

        LOGGER.info("Starting PCA job...");

        final JavaSparkContext js = JavaSparkContext.fromSparkContext(sparkContext);
        final JavaRDD<Row> inputRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());

        final JavaRDD<Row> withoutMissingValues;
        if (input.failOnMissingValues()) {
            withoutMissingValues = RDDUtilsInJava.failOnMissingValues(inputRDD, input.getColumnIdxs(),
                "Missing value in input column with index '%d' detected.");
        } else {
            withoutMissingValues = RDDUtilsInJava.dropMissingValues(inputRDD, input.getColumnIdxs());
        }

        final RowMatrix inputAsMatrix = RDDUtilsInJava.toRowMatrix(withoutMissingValues, input.getColumnIdxs());
        final Matrix pc;
        try {
            pc = inputAsMatrix.computePrincipalComponents(input.getTargetDimensions());
        } catch (NotConvergedException e) {
            throw new KNIMESparkException(toUserString(e), e);
        }

        validatePrincipalComponents(pc);

        final JavaRDD<Row> reducedValues = RDDUtilsInJava.fromRowMatrix(inputAsMatrix.multiply(pc));
        final JavaRDD<Row> resultProj;
        switch(input.getMode()) {
            case APPEND_COLUMNS:
                resultProj = RDDUtilsInJava.mergeRows(withoutMissingValues.zip(reducedValues));
                break;
            case REPLACE_COLUMNS:
                resultProj = RDDUtilsInJava.mergeRows(withoutMissingValues.zip(reducedValues), input.getColumnIdxs());
                break;
            case LEGACY:
                resultProj = reducedValues;
                break;
            default:
                throw new KNIMESparkException("Unknown mode");
        }
        namedObjects.addJavaRdd(input.getProjectionOutputName(), resultProj);

        final JavaRDD<Row> resultPCMatrix = RDDUtilsInJava.fromMatrix(js, pc);
        namedObjects.addJavaRdd(input.getPCMatrixOutputName(), resultPCMatrix);

        LOGGER.info("PCA job done.");
        return new PCAJobOutput(input.getTargetDimensions());
    }

    /**
     * Validates if computed eigenvectors contains NaN values.
     *
     * @throws KNIMESparkException on <code>NaN</code> values
     */
    private void validatePrincipalComponents(final Matrix pc) throws KNIMESparkException {
        for (double val : pc.toArray()) {
            if (Double.isNaN(val)) {
                throw new KNIMESparkException("Unable to run principal components analysis (PC contains NaN values).");
            }
        }
    }

    private String toUserString(final NotConvergedException e) {
        if (e.reason() instanceof NotConvergedException.Breakdown$) {
            return "Principal components analysis failed to converge (Breakdown).";
        } else if (e.reason() instanceof NotConvergedException.Divergence$) {
            return "Principal components analysis failed to converge (Divergence).";
        } else if (e.reason() instanceof NotConvergedException.Iterations$) {
            return "Principal components analysis failed to converge (Iterations).";
        } else {
            return "Principal components analysis failed to converge.";
        }
    }
}
