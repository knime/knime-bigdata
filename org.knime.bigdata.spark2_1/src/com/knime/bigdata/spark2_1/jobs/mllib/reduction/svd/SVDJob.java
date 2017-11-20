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
package com.knime.bigdata.spark2_1.jobs.mllib.reduction.svd;


import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.mllib.reduction.svd.SVDJobInput;
import com.knime.bigdata.spark.node.mllib.reduction.svd.SVDJobOutput;
import com.knime.bigdata.spark2_1.api.NamedObjects;
import com.knime.bigdata.spark2_1.api.RDDUtilsInJava;
import com.knime.bigdata.spark2_1.api.SparkJob;

/**
 *
 * @author dwk
 */
@SparkClass
public class SVDJob implements SparkJob<SVDJobInput, SVDJobOutput> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(SVDJob.class.getName());

    @Override
    public SVDJobOutput runJob(final SparkContext sparkContext, final SVDJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {
        LOGGER.info("Starting SVD job...");

        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());

     // Create a RowMatrix from JavaRDD<Row>.
        final RowMatrix mat = RDDUtilsInJava.toRowMatrix(rowRDD, input.getColumnIdxs());

        // Compute the top k singular values and corresponding singular vectors.
        final SingularValueDecomposition<RowMatrix, Matrix> svd;

        try {
            svd = mat.computeSVD(input.getK(), input.computeU(), input.getRCond());
        } catch (Exception e) {
            throw new KNIMESparkException(e);
        }

        final SVDJobOutput jobOutput = new SVDJobOutput(svd.s().toArray());

        final JavaSparkContext js = JavaSparkContext.fromSparkContext(sparkContext);
        final Dataset<Row> V = RDDUtilsInJava.fromMatrix(js, svd.V());
        namedObjects.addDataFrame(input.getVMatrixName(), V);

        if (input.computeU()) {
            final Dataset<Row> U = RDDUtilsInJava.fromRowMatrix(sparkContext, svd.U());
            namedObjects.addDataFrame(input.getUMatrixName(), U);
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

        LOGGER.info("SVD job done.");
        return jobOutput;
    }
}
