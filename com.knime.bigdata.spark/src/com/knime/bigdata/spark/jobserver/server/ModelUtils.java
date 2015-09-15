package com.knime.bigdata.spark.jobserver.server;

import java.util.List;
import java.util.logging.Logger;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.api.java.Row;

import scala.Tuple2;

import com.knime.bigdata.spark.jobserver.jobs.CollaborativeFilteringJob;

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
        if (aModel instanceof MatrixFactorizationModel) {

            LOGGER.fine("MatrixFactorizationModel (Collaborative Filtering) found for prediction");
            JavaRDD<Rating> ratings = CollaborativeFilteringJob.convertRowRDD2RatingsRdd(aConfig, aRowRDD);
            return predict(aRowRDD, ratings, (MatrixFactorizationModel)aModel);
        } else {
            return predict(RDDUtils.toJavaRDDOfVectorsOfSelectedIndices(aRowRDD, aColIdxs), aRowRDD, aModel);
        }
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
    public static <T> JavaRDD<Row> predict(final JavaRDD<Vector> aNumericData,
        final JavaRDD<Row> rowRDD, final T aModel) throws GenericKnimeSparkException {
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
        } else if (aModel instanceof SVMModel) {
            LOGGER.fine("SVMModel found for prediction");
            predictions = ((SVMModel)aModel).predict(aNumericData);
        } else if (aModel instanceof LinearRegressionModel) {
            LOGGER.fine("LinearRegressionModel found for prediction");
            predictions = ((LinearRegressionModel)aModel).predict(aNumericData);
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
     * @param aRatings
     * @param aModel
     * @return original data plus one new column with predicted ratings
     */
    public static JavaRDD<Row> predict(final JavaRDD<Row> aRowRdd, final JavaRDD<Rating> aRatings, final MatrixFactorizationModel aModel) {

        // Evaluate the model on rating data
        final JavaRDD<Tuple2<Object, Object>> userProducts =
            aRatings.map(new Function<Rating, Tuple2<Object, Object>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<Object, Object> call(final Rating r) {
                    return new Tuple2<Object, Object>(r.user(), r.product());
                }
            });
        JavaRDD<Double> predictions = aModel.predict(userProducts.rdd()).toJavaRDD().map(new Function<Rating, Double>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Double call(final Rating r) {
                return r.rating();
            }
        });
        return RDDUtils.addColumn(aRowRdd.zip(predictions));

        //        return aModel.predict(userProducts.rdd()).toJavaRDD().map(new Function<Rating, Row>() {
        //            private static final long serialVersionUID = 1L;
        //
        //            @Override
        //            public Row call(final Rating r) {
        //                RowBuilder b = RowBuilder.emptyRow();
        //                b.add(r.user());
        //                b.add(r.product());
        //                b.add(r.rating());
        //                return b.build();
        //            }
        //        });
    }
}
