package com.knime.bigdata.spark.jobserver.server;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.Row;

import com.google.common.base.Optional;

import scala.Tuple2;

/**
 *
 * model serialization and application utility (until Spark supports PMML import and export, hopefully with 1.4)
 */
public class ModelUtils {

    private final static Logger LOGGER = Logger.getLogger(ModelUtils.class.getName());

    /**
     * apply a model to the given data
     *
     * @param aConfig
     * @param aRowRDD - original data
     * @param aColIdxs - indices of columns to be used for prediction
     * @param aModel - model to be applied
     * @return original data with appended column containing predictions
     * @throws GenericKnimeSparkException thrown when model type cannot be handled
     */
    public static <T> JavaRDD<Row> predict(final JobConfig aConfig, final JavaRDD<Row> aRowRDD,
        final List<Integer> aColIdxs, final T aModel) throws GenericKnimeSparkException {
        //        if (aModel instanceof CollaborativeFilteringModel) {
        //            final MatrixFactorizationModel model =
        //                CollaborativeFilteringModelFactory.fromCollaborativeFilteringModel((CollaborativeFilteringModel)aModel);
        //            LOGGER.fine("MatrixFactorizationModel (Collaborative Filtering) found for prediction");
        //            final JavaRDD<Rating> ratings = CollaborativeFilteringJob.convertRowRDD2RatingsRdd(aConfig, aRowRDD);
        //            return predict(aRowRDD, ratings, model);
        return predict(RDDUtils.toJavaRDDOfVectorsOfSelectedIndices(aRowRDD, aColIdxs), aRowRDD, aModel);
    }

    /**
     * apply a model to the given data
     *
     * @param aNumericData - data to be used for prediction (must be compatible to the model)
     * @param rowRDD - original data
     * @param aModel - model to be applied
     * @return original data with appended column containing predictions
     * @throws GenericKnimeSparkException thrown when model type cannot be handled
     */
    public static <T> JavaRDD<Row>
        predict(final JavaRDD<Vector> aNumericData, final JavaRDD<Row> rowRDD, final T aModel)
            throws GenericKnimeSparkException {
        //use only the column indices when converting to vector
        aNumericData.cache();

        final JavaRDD<? extends Object> predictions;
        if (aModel instanceof KMeansModel) {
            LOGGER.fine("KMeansModel found for prediction");
            predictions = ((KMeansModel)aModel).predict(aNumericData);
        } else if (aModel instanceof DecisionTreeModel) {
            LOGGER.fine("DecisionTreeModel found for prediction");
            predictions = ((DecisionTreeModel)aModel).predict(aNumericData);
        } else if (aModel instanceof RandomForestModel) {
            LOGGER.fine("RandomForestModel found for prediction");
            predictions = ((RandomForestModel)aModel).predict(aNumericData);
        } else if (aModel instanceof GradientBoostedTreesModel) {
            LOGGER.fine("GradientBoostedTreesModel found for prediction");
            predictions = ((GradientBoostedTreesModel)aModel).predict(aNumericData);
        } else if (aModel instanceof SVMModel) {
            LOGGER.fine("SVMModel found for prediction");
            predictions = ((SVMModel)aModel).predict(aNumericData);
        } else if (aModel instanceof LinearRegressionModel) {
            LOGGER.fine("LinearRegressionModel found for prediction");
            predictions = ((LinearRegressionModel)aModel).predict(aNumericData);
        } else if (aModel instanceof LogisticRegressionModel) {
            LOGGER.fine("LogisticRegressionModel found for prediction");
            predictions = ((LogisticRegressionModel)aModel).predict(aNumericData);
        } else if (aModel instanceof NaiveBayesModel) {
            LOGGER.fine("NaiveBayesModel found for prediction");
            predictions = ((NaiveBayesModel)aModel).predict(aNumericData);
        } else {
            throw new GenericKnimeSparkException("ERROR: unknown model type: " + aModel.getClass());
        }

        return RDDUtils.addColumn(rowRDD.zip(predictions));
    }

    /**
     *
     * @param aRowRdd
     * @param aUserProductInfo
     * @param aProductIdx
     * @param aUserIdx
     * @param aModel
     * @return original data plus one new column with predicted ratings
     */
    public static JavaRDD<Row> predict(final JavaRDD<Row> aRowRdd, final JavaRDD<Rating> aUserProductInfo,
        final int aUserIdx, final int aProductIdx, final MatrixFactorizationModel aModel) {

        // apply the model to user/product data
        final JavaPairRDD<Integer, Integer> userProducts =
            JavaPairRDD.fromJavaRDD(aUserProductInfo.map(new Function<Rating, Tuple2<Integer, Integer>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<Integer, Integer> call(final Rating r) {
                    return new Tuple2<Integer, Integer>(r.user(), r.product());
                }
            }));
        final JavaRDD<Rating> userProductsAndPredictions = aModel.predict(userProducts);

        final JavaRDD<Row> predictions = RDDUtilsInJava.convertRatings2RowRDDRdd(userProductsAndPredictions);
        // join again with original data, use left outer join since predict does not necessarily return a prediction for each
        // record
        final JavaPairRDD<MyJoinKey, Row> leftRdd =
            RDDUtilsInJava.extractKeys(aRowRdd, new Integer[]{aUserIdx, aProductIdx});

        final JavaPairRDD<MyJoinKey, Row> rightRdd = RDDUtilsInJava.extractKeys(predictions, new Integer[]{0, 1});
        final JavaRDD<Tuple2<Row, Optional<Row>>> joinedRdd = leftRdd.leftOuterJoin(rightRdd).values();
        final List<Integer> predictedRatingIdx = new ArrayList<>();
        predictedRatingIdx.add(2);
        final int nFeatures = aRowRdd.take(1).get(0).length();
        final List<Integer> origIdx = new ArrayList<>();
        for (int i = 0; i < nFeatures; i++) {
            origIdx.add(i);
        }
        return RDDUtilsInJava.mergeRows(joinedRdd, origIdx, predictedRatingIdx);

    }
}
