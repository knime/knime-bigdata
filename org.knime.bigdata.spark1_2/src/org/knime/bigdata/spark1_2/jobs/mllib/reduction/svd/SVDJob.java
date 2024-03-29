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
package org.knime.bigdata.spark1_2.jobs.mllib.reduction.svd;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.api.java.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.reduction.svd.SVDJobInput;
import org.knime.bigdata.spark.node.mllib.reduction.svd.SVDJobOutput;
import org.knime.bigdata.spark1_2.api.NamedObjects;
import org.knime.bigdata.spark1_2.api.RDDUtilsInJava;
import org.knime.bigdata.spark1_2.api.SparkJob;

/**
 *
 * @author dwk
 */
@SparkClass
public class SVDJob implements SparkJob<SVDJobInput, SVDJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(SVDJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("resource")
    @Override
    public SVDJobOutput runJob(final SparkContext sparkContext, final SVDJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {
        LOGGER.log(Level.INFO, "starting SVD job...");

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
        final JavaRDD<Row> V = RDDUtilsInJava.fromMatrix(js, svd.V());
        namedObjects.addJavaRdd(input.getVMatrixName(), V);

        if (input.computeU()) {
            final JavaRDD<Row> U = RDDUtilsInJava.fromRowMatrix(svd.U());
            namedObjects.addJavaRdd(input.getUMatrixName(), U);
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
        return jobOutput;
    }
}
