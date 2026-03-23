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
package org.knime.bigdata.spark3_5.jobs.mllib.clustering.kmeans;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.ModelJobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.clustering.kmeans.KMeansJobInput;
import org.knime.bigdata.spark.node.mllib.prediction.predictor.PredictionJobInput;
import org.knime.bigdata.spark3_5.api.ModelUtils;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;

/**
 * Runs KMeans on a given data frame and returns model as result
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class KMeansJob implements SparkJob<KMeansJobInput, ModelJobOutput> {
    private final static long serialVersionUID = 1L;
    private final static Logger LOGGER = Logger.getLogger(KMeansJob.class.getName());

    @Override
    public ModelJobOutput runJob(final SparkContext sparkContext, final KMeansJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        LOGGER.info("Starting KMeans job...");

        final String namedInputObject = input.getFirstNamedInputObject();
        final String tmpFeatureColumn = ModelUtils.getTemporaryColumnName("features");
        final Dataset<Row> ds = namedObjects.getDataFrame(namedInputObject);
        final VectorAssembler va = new VectorAssembler()
                .setInputCols(input.getColumnNames(ds.columns()))
                .setOutputCol(tmpFeatureColumn);
        final Dataset<Row> vectors = va.transform(ds).cache();
        final KMeans kMeans = new KMeans()
                .setFeaturesCol(tmpFeatureColumn)
                .setPredictionCol(input.getPredictionColumnName())
                .setK(input.getNoOfClusters())
                .setMaxIter(input.getNoOfIterations())
                .setSeed(input.getSeed());
        final KMeansModel model = kMeans.fit(vectors);
        final Dataset<Row> output = model.transform(vectors).drop(tmpFeatureColumn);
        vectors.unpersist();

        LOGGER.info("KMeans done");
        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), output);

        return new ModelJobOutput(model);
    }

    /**
     * Assign clusters from model to input data.
     *
     * @param input job configuration with input column names
     * @param inputDataset input data
     * @param model KMeans model
     * @return input data with assigned clusters
     */
    public static Dataset<Row> transform(final PredictionJobInput input, final Dataset<Row> inputDataset,
            final KMeansModel model) {

        final String tmpFeatureColumn = ModelUtils.getTemporaryColumnName("features");
        final VectorAssembler va = new VectorAssembler()
                .setInputCols(input.getIncludeColumnNames(inputDataset.columns()))
                .setOutputCol(tmpFeatureColumn);
        final Dataset<Row> vectors = va.transform(inputDataset).cache();
        model.setFeaturesCol(tmpFeatureColumn);
        model.setPredictionCol(input.getPredictionColumnName());
        return model.transform(vectors).drop(tmpFeatureColumn);
    }
}
