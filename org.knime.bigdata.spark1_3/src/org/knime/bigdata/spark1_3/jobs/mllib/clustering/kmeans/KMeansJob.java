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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark1_3.jobs.mllib.clustering.kmeans;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.ModelJobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.clustering.kmeans.KMeansJobInput;
import org.knime.bigdata.spark1_3.api.ModelUtils;
import org.knime.bigdata.spark1_3.api.NamedObjects;
import org.knime.bigdata.spark1_3.api.SparkJob;

import com.knime.bigdata.spark.jobserver.server.RDDUtils;

/**
 * runs MLlib KMeans on a given RDD, model is returned as result
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
@SparkClass
public class KMeansJob implements SparkJob<KMeansJobInput, ModelJobOutput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(KMeansJob.class.getName());

    /** default number of parallel runs */
    private static final int DEFAULT_RUNS = 1;

    /** default initialization mode */
    private static final String DEFAULT_MODE = KMeans.K_MEANS_PARALLEL();

    @Override
    public ModelJobOutput runJob(final SparkContext sparkContext, final KMeansJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        LOGGER.log(Level.INFO, "Starting kMeans job...");

        final JavaRDD<Row> rowRdd = namedObjects.getJavaRdd(input.getNamedInputObjects().get(0));

        //use only the column indices when converting to vector
        final JavaRDD<Vector> vectorRdd =
            RDDUtils.toJavaRDDOfVectorsOfSelectedIndices(rowRdd, input.getColumnIdxs());
        vectorRdd.cache();
        // Cluster the data into m_noOfCluster classes using KMeans
        final KMeansModel model = KMeans.train(vectorRdd.rdd(), input.getNoOfClusters(),
            input.getNoOfIterations(), DEFAULT_RUNS, DEFAULT_MODE, input.getSeed());

        final JavaRDD<Row> predictedData = ModelUtils.predict(vectorRdd, rowRdd, model);
        namedObjects.addJavaRdd(input.getNamedOutputObjects().get(0), predictedData);

        LOGGER.log(Level.INFO, "kMeans done");
        return new ModelJobOutput(model);
    }
}
