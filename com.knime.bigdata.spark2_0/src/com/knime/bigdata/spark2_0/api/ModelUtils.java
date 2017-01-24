package com.knime.bigdata.spark2_0.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.MyJoinKey;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;

import scala.Tuple2;

/**
 *
 * model serialization and application utility (until Spark supports PMML import and export, hopefully with 1.4)
 */
@SparkClass
public class ModelUtils {

    private final static Logger LOGGER = Logger.getLogger(ModelUtils.class.getName());

    /**
     * @param includeColumns - name of columns to be used for prediction
     * @param dataFrame - original data
     * @param model - model to be applied
     * @param predictionColumn - result column with predictions
     * @return original data with appended column containing predictions
     * @throws Exception thrown when model type cannot be handled
     */
    public static <T> Dataset<Row> predict(final String includeColumns[], final Dataset<Row> dataFrame,
            final T model, final String predictionColumn) throws Exception {

        final String tmpFeatureCol = "features_" + Long.toHexString(Math.abs(new Random().nextLong()));
        final VectorAssembler va = new VectorAssembler()
                .setInputCols(includeColumns)
                .setOutputCol(tmpFeatureCol);
        final Dataset<Row> vectors = va.transform(dataFrame);
        final Dataset<Row> predictions = predict(vectors, model, tmpFeatureCol, predictionColumn);
        return predictions.drop(tmpFeatureCol);
    }

    /**
     * apply a model to the given data
     *
     * @param vectors - data to be used for prediction (must contain a feature column)
     * @param model - model to be applied
     * @param featureColumn - column with input features
     * @param predictionColumn - result column with predictions
     * @return original data with appended column containing predictions
     * @throws Exception thrown when model type cannot be handled
     */
    public static <T> Dataset<Row> predict(final Dataset<Row> vectors, final T model,
            final String featureColumn, final String predictionColumn) throws Exception {
        vectors.cache();

        final Dataset<Row> predictions;
        if (model instanceof KMeansModel) {
            LOGGER.fine("KMeansModel found for prediction");
            LOGGER.warning("KMeans with feature " + featureColumn + " and prediction " + predictionColumn + " and " + vectors.schema());

            ((KMeansModel) model).setFeaturesCol(featureColumn);
            ((KMeansModel) model).setPredictionCol(predictionColumn);
            predictions = ((KMeansModel) model).transform(vectors);
//        } else if (model instanceof DecisionTreeModel) {
//            LOGGER.fine("DecisionTreeModel found for prediction");
//            ((DecisionTreeModel)model).setPredictionCol(predictionColumn);
//            predictions = ((DecisionTreeModel)model).transform(vectors);
//        } else if (model instanceof RandomForestModel) {
//            LOGGER.fine("RandomForestModel found for prediction");
//            ((RandomForestModel)model).setPredictionCol(predictionColumn);
//            predictions = ((RandomForestModel)model).transform(vectors);
//        } else if (model instanceof GradientBoostedTreesModel) {
//            LOGGER.fine("GradientBoostedTreesModel found for prediction");
//            ((GradientBoostedTreesModel)model).setPredictionCol(predictionColumn);
//            predictions = ((GradientBoostedTreesModel)model).transform(vectors);
//        } else if (model instanceof SVMModel) {
//            LOGGER.fine("SVMModel found for prediction");
//            ((SVMModel)model).setPredictionCol(predictionColumn);
//            predictions = ((SVMModel)model).transform(vectors);
        } else if (model instanceof LinearRegressionModel) {
            LOGGER.fine("LinearRegressionModel found for prediction");
            ((LinearRegressionModel)model).setPredictionCol(predictionColumn);
            predictions = ((LinearRegressionModel)model).transform(vectors);
        } else if (model instanceof LogisticRegressionModel) {
            LOGGER.fine("LogisticRegressionModel found for prediction");
            ((LogisticRegressionModel)model).setPredictionCol(predictionColumn);
            predictions = ((LogisticRegressionModel)model).transform(vectors);
        } else if (model instanceof NaiveBayesModel) {
            LOGGER.fine("NaiveBayesModel found for prediction");
            ((NaiveBayesModel)model).setPredictionCol(predictionColumn);
            predictions = ((NaiveBayesModel)model).transform(vectors);
        } else {
            throw new Exception("Unknown model type: " + model.getClass());
        }

        return predictions;
    }


    /**
     * @param includeColumnIndices indices of columns to be used for prediction
     * @param rowRdd original data
     * @param model model to be applied
     * @return original data with appended column containing predictions
     * @throws Exception thrown when model type cannot be handled
     */
    @Deprecated
    public static <T> JavaRDD<Row> predict(final List<Integer> includeColumnIndices, final JavaRDD<Row> rowRdd,
        final T model) throws Exception {
      //use only the column indices when converting to vector
        final JavaRDD<Vector> vectorRdd =
            RDDUtils.toJavaRDDOfVectorsOfSelectedIndices(rowRdd, includeColumnIndices);
        return predict(vectorRdd, rowRdd, model);
    }

    /**
     * apply a model to the given data
     *
     * @param aNumericData - data to be used for prediction (must be compatible to the model)
     * @param rowRDD - original data
     * @param aModel - model to be applied
     * @return original data with appended column containing predictions
     * @throws Exception thrown when model type cannot be handled
     */
    @Deprecated
    public static <T> JavaRDD<Row> predict(final JavaRDD<Vector> aNumericData,
        final JavaRDD<Row> rowRDD, final T aModel) throws Exception {
        //use only the column indices when converting to vector
        aNumericData.cache();

        final JavaRDD<? extends Object> predictions;
        if (aModel instanceof org.apache.spark.mllib.clustering.KMeansModel) {
            LOGGER.fine("KMeansModel found for prediction");
            predictions = ((org.apache.spark.mllib.clustering.KMeansModel)aModel).predict(aNumericData);
        } else if (aModel instanceof org.apache.spark.mllib.tree.model.DecisionTreeModel) {
            LOGGER.fine("DecisionTreeModel found for prediction");
            predictions = ((org.apache.spark.mllib.tree.model.DecisionTreeModel)aModel).predict(aNumericData);
        } else if (aModel instanceof org.apache.spark.mllib.tree.model.RandomForestModel) {
            LOGGER.fine("RandomForestModel found for prediction");
            predictions = ((org.apache.spark.mllib.tree.model.RandomForestModel)aModel).predict(aNumericData);
        } else if (aModel instanceof org.apache.spark.mllib.tree.model.GradientBoostedTreesModel) {
            LOGGER.fine("GradientBoostedTreesModel found for prediction");
            predictions = ((org.apache.spark.mllib.tree.model.GradientBoostedTreesModel)aModel).predict(aNumericData);
        } else if (aModel instanceof org.apache.spark.mllib.classification.SVMModel) {
            LOGGER.fine("SVMModel found for prediction");
            predictions = ((org.apache.spark.mllib.classification.SVMModel)aModel).predict(aNumericData);
        } else if (aModel instanceof org.apache.spark.mllib.regression.LinearRegressionModel) {
            LOGGER.fine("LinearRegressionModel found for prediction");
            predictions = ((org.apache.spark.mllib.regression.LinearRegressionModel)aModel).predict(aNumericData);
        } else if (aModel instanceof org.apache.spark.mllib.classification.LogisticRegressionModel) {
            LOGGER.fine("LogisticRegressionModel found for prediction");
            predictions = ((org.apache.spark.mllib.classification.LogisticRegressionModel)aModel).predict(aNumericData);
        } else if (aModel instanceof org.apache.spark.mllib.classification.NaiveBayesModel) {
            LOGGER.fine("NaiveBayesModel found for prediction");
            predictions = ((org.apache.spark.mllib.classification.NaiveBayesModel)aModel).predict(aNumericData);
        } else {
            throw new Exception("Unknown model type: " + aModel.getClass());
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
    @Deprecated
    public static JavaRDD<Row> predict(final JavaRDD<Row> aRowRdd, final JavaRDD<Rating> aUserProductInfo,
        final int aUserIdx, final int aProductIdx, final MatrixFactorizationModel aModel) {
       // apply the model to user/product data
       final JavaPairRDD<Integer, Integer> userProducts =
           JavaPairRDD.fromJavaRDD(aUserProductInfo.map(new Function<Rating, Tuple2<Integer, Integer>>() {
               private static final long serialVersionUID = 1L;
               @Override
               public Tuple2<Integer, Integer> call(final Rating r) {
                   return new Tuple2<>(r.user(), r.product());
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

    /**
     * @param prefix - column name prefix
     * @return prefix concatenated with a random hex number
     */
    public static String getTemporaryColumnName(final String prefix) {
        return prefix + "_" + Long.toHexString(Math.abs(new Random().nextLong()));
    }
}
