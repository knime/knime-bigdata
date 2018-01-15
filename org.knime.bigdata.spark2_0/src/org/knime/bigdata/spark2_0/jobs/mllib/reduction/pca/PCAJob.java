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
 *   Created on 12.08.2015 by dwk
 */
package org.knime.bigdata.spark2_0.jobs.mllib.reduction.pca;

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
import org.knime.bigdata.spark2_0.api.NamedObjects;
import org.knime.bigdata.spark2_0.api.RDDUtilsInJava;
import org.knime.bigdata.spark2_0.api.SimpleSparkJob;

import scala.Tuple2;

/**
 *
 * @author dwk
 */
@SparkClass
public class PCAJob implements SimpleSparkJob<PCAJobInput> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(PCAJob.class.getName());

    @Override
    public void runJob(final SparkContext sparkContext, final PCAJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {

        LOGGER.info("Starting PCA job...");

        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());

        // Create a RowMatrix from JavaRDD<Row>.
        final RowMatrix mat = RDDUtilsInJava.toRowMatrix(rowRDD, input.getColumnIdxs());
        // Compute the top k singular values and corresponding singular vectors.
        final Tuple2<RowMatrix, Matrix> pcaRes = new Tuple2<>(mat, mat.computePrincipalComponents(input.getK()));

        final String matrixName = input.getMatrixName();
        if (matrixName != null) {
            final JavaSparkContext js = JavaSparkContext.fromSparkContext(sparkContext);
            namedObjects.addDataFrame(matrixName, RDDUtilsInJava.fromMatrix(js, pcaRes._2));
        }

        final String projectionMatrix = input.getProjectionMatrix();
        if (projectionMatrix != null) {
            // Project the rows to the linear space spanned by the top N principal components.
            final RowMatrix projected = pcaRes._1.multiply(pcaRes._2);
            namedObjects.addDataFrame(projectionMatrix,  RDDUtilsInJava.fromRowMatrix(sparkContext, projected));
        }

        return;
    }
}