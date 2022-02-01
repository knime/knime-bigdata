package org.knime.bigdata.spark3_2.api;

import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.job.SparkClass;

import scala.Tuple2;

/**
 *
 * model serialization and application utility (until Spark supports PMML import and export, hopefully with 1.4)
 */
@SparkClass
public class ModelUtils {

    private final static Logger LOGGER = Logger.getLogger(ModelUtils.class.getName());
    private final static Random RAND = new Random();

    /**
     * @param includeColumnIndices indices of columns to be used for prediction
     * @param dataset original data
     * @param model model to be applied
     * @return original data with appended column containing predictions
     * @throws Exception thrown when model type cannot be handled
     */
    @Deprecated
    public static <T> JavaRDD<Row> predict(final List<Integer> includeColumnIndices, final Dataset<Row> dataset,
        final T model) throws Exception {
      //use only the column indices when converting to vector
        final JavaRDD<Vector> vectorRdd = RDDUtilsInJava.toVectorRdd(dataset, includeColumnIndices);
        return predict(vectorRdd, dataset.javaRDD(), model);
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
            throw new Exception("Unknown model type: " + aModel.getClass());
        }

        return addColumn(rowRDD.zip(predictions));
    }

    /**
     * Appends a value as new column to a row.
     *
     * @param pairs pair of row and a value
     * @return row with appended value
     */
    private static <T> JavaRDD<Row> addColumn(final JavaPairRDD<Row, T> pairs) {
        return pairs.map(new Function<Tuple2<Row, T>, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final Tuple2<Row, T> pair) throws Exception {
                return RowBuilder.fromRow(pair._1).add(pair._2).build();
            }
        });
    }

    /**
     * @param prefix - column name prefix
     * @return prefix concatenated with a random hex number
     */
    public static String getTemporaryColumnName(final String prefix) {
        return prefix + "_" + Long.toHexString(RAND.nextLong());
    }
}
