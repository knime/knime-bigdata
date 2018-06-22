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
package org.knime.bigdata.spark2_3.jobs.mllib.reduction.svd;


import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.reduction.svd.SVDJobInput;
import org.knime.bigdata.spark.node.mllib.reduction.svd.SVDJobOutput;
import org.knime.bigdata.spark2_3.api.NamedObjects;
import org.knime.bigdata.spark2_3.api.RDDUtilsInJava;
import org.knime.bigdata.spark2_3.api.SparkJob;

/**
 *
 * @author dwk
 */
@SparkClass
public class SVDJob implements SparkJob<SVDJobInput, SVDJobOutput> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(SVDJob.class.getName());

    private static final String COL_PREFIX = "Dimension ";

    @Override
    public SVDJobOutput runJob(final SparkContext sparkContext, final SVDJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {
        LOGGER.info("Starting SVD job...");

        final Dataset<Row> dataset = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        final RowMatrix inputMatrix = RDDUtilsInJava.toRowMatrix(dataset, input.getColumnIdxs());
        final SingularValueDecomposition<RowMatrix, Matrix> svd = inputMatrix.computeSVD(input.getK(), input.computeU(), input.getRCond());
        final SVDJobOutput jobOutput = new SVDJobOutput(svd.s().toArray());

        final Dataset<Row> v = RDDUtilsInJava.fromMatrix(sparkContext, svd.V(), COL_PREFIX);
        namedObjects.addDataFrame(input.getVMatrixName(), v);

        if (input.computeU()) {
            final Dataset<Row> u = RDDUtilsInJava.fromRowMatrix(sparkContext, svd.U(), COL_PREFIX);
            namedObjects.addDataFrame(input.getUMatrixName(), u);
        }

        LOGGER.info("SVD job done.");
        return jobOutput;
    }
}
