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
package org.knime.bigdata.spark3_0.jobs.mllib.reduction.pca;

import java.util.Arrays;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.reduction.pca.PCAJobInput;
import org.knime.bigdata.spark.node.mllib.reduction.pca.PCAJobOutput;
import org.knime.bigdata.spark3_0.api.NamedObjects;
import org.knime.bigdata.spark3_0.api.RDDUtilsInJava;
import org.knime.bigdata.spark3_0.api.SparkJob;

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

        final String tmpInputCol = "inputFeatures_" + UUID.randomUUID();
        final String tmpPCACol = "pcaFeatures_" + UUID.randomUUID();

        final Dataset<Row> inputDataset =
            validateInput(namedObjects.getDataFrame(input.getFirstNamedInputObject()), input);

        final VectorAssembler va = new VectorAssembler()
          .setInputCols(input.getColumnNames(inputDataset.columns()))
          .setOutputCol(tmpInputCol);
        final Dataset<Row> inputWithVector = va.transform(inputDataset);

        final PCA pca = new PCA()
          .setInputCol(tmpInputCol)
          .setOutputCol(tmpPCACol);
        final PCAModel pcaModel;

        try {
            if (input.computeTargetDimension()) {
                pca.setK(input.getColumnIdxs().size());
                pcaModel = selectComponentsByQuality(pca.fit(inputWithVector), input.getMinQuality());
            } else {
                pca.setK(input.getTargetDimensions());
                pcaModel = pca.fit(inputWithVector);
            }
        } catch (NotConvergedException e) {
            throw new KNIMESparkException(toUserString(e), e);
        }

        validatePrincipalComponents(pcaModel.pc());

        final int noOfComponents = pcaModel.pc().numCols();
        final Dataset<Row> withPCAVector;
        switch(input.getMode()) {
            case APPEND_COLUMNS:
                withPCAVector = pcaModel.transform(inputWithVector)
                    .drop(tmpInputCol);
                break;
            case REPLACE_COLUMNS:
                withPCAVector = pcaModel.transform(inputWithVector)
                    .drop(tmpInputCol)
                    .drop(input.getColumnNames(inputDataset.columns()));
                break;
            case LEGACY:
                withPCAVector = pcaModel.transform(inputWithVector)
                    .select(tmpPCACol);
                break;
            default:
                throw new KNIMESparkException("Unknown mode");
        }

        final Dataset<Row> resultProj = RDDUtilsInJava.explodeVector(withPCAVector, tmpPCACol, noOfComponents, input.getProjColPrefix());
        namedObjects.addDataFrame(input.getProjectionOutputName(), resultProj);
        final Dataset<Row> resultPCMatrix = RDDUtilsInJava.fromMatrix(sparkContext, pcaModel.pc(), input.getCompColPrefix());
        namedObjects.addDataFrame(input.getPCMatrixOutputName(), resultPCMatrix);

        LOGGER.info("PCA job done.");
        return new PCAJobOutput(noOfComponents);
    }

    /**
     * @return input data set without missing values if missing values should be ignored
     * @throws KNIMESparkException if input contains missing values and job should fail on missing values
     * @throws KNIMESparkException if input columns are not numeric
     */
    private Dataset<Row> validateInput(final Dataset<Row> inputDataset, final PCAJobInput input) throws KNIMESparkException {
        RDDUtilsInJava.failOnNonNumericColumn(inputDataset, input.getColumnIdxs(),
            "Unsupported non numeric input column '%s'.");

        if (input.failOnMissingValues()) {
            return RDDUtilsInJava.failOnMissingValues(inputDataset, input.getColumnIdxs(),
                "Missing value in input column '%s' detected.");
        } else {
            return inputDataset.na().drop(input.getColumnNames(inputDataset.columns()));
        }
    }

    private PCAModel selectComponentsByQuality(final PCAModel pcaModel, final double minQuality) {
        final int rows = pcaModel.pc().numRows();

        // find number of required components
        final DenseVector variance = pcaModel.explainedVariance();
        int noOfComponents = 1;
        double quality = variance.apply(0);
        for (; noOfComponents < variance.size() && quality < minQuality; noOfComponents++) {
            quality += variance.apply(noOfComponents);
        }

        // extract principal components matrix
        final DenseVector newVariance = new DenseVector(Arrays.copyOf(variance.toArray(), noOfComponents));
        final double oldPC[] = pcaModel.pc().toArray();
        final double newPC[] = Arrays.copyOf(oldPC, rows * noOfComponents);
        final DenseMatrix newPCMatrix = new DenseMatrix(rows, noOfComponents, newPC);

        // create new model
        final String modelUid = "pca_" + UUID.randomUUID().toString().substring(0, 12);
        final ParamMap params = pcaModel.extractParamMap();
        params.put(pcaModel.k().w(noOfComponents));
        return pcaModel.copyValues(new PCAModel(modelUid, newPCMatrix, newVariance), params);
    }

    /**
     * Validates if computed eigenvectors contains NaN values.
     *
     * @throws KNIMESparkException on <code>NaN</code> values
     */
    private void validatePrincipalComponents(final DenseMatrix pc) throws KNIMESparkException {
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
