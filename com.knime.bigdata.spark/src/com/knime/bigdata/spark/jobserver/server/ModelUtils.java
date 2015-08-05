package com.knime.bigdata.spark.jobserver.server;

import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.sql.api.java.Row;

/**
 *
 * model serialization and application utility (until Spark supports PMML import and export, hopefully with 1.4)
 */
public class ModelUtils {

	private final static Logger LOGGER = Logger.getLogger(ModelUtils.class
			.getName());

	/**
	 * apply a model to the given data
	 * @param aContext
	 * @param aNumericData - data to be used for prediction (must be compatible to the model)
	 * @param rowRDD - original data
	 * @param aModel - model to be applied
	 * @return original data with appended column containing predictions
	 * @throws GenericKnimeSparkException thrown when model type cannot be handled
	 */
    public static <T> JavaRDD<Row> predict(final SparkContext aContext, final JavaRDD<Vector> aNumericData,
        final JavaRDD<Row> rowRDD, final T aModel) throws GenericKnimeSparkException {
        aNumericData.cache();

        final JavaRDD<? extends Object> predictions;
        if (aModel instanceof KMeansModel) {
            LOGGER.fine("KMeansModel found for prediction");
            predictions = ((KMeansModel)aModel).predict(aNumericData);
        } else if (aModel instanceof DecisionTreeModel) {
            LOGGER.fine("DecisionTreeModel found for prediction");
            predictions = ((DecisionTreeModel)aModel).predict(aNumericData);
        } else if (aModel instanceof SVMModel) {
            LOGGER.fine("SVMModel found for prediction");
            predictions = ((SVMModel)aModel).predict(aNumericData);
        } else if (aModel instanceof LinearRegressionModel) {
            LOGGER.fine("LinearRegressionModel found for prediction");
            predictions = ((LinearRegressionModel)aModel).predict(aNumericData);
        } else {
            throw new GenericKnimeSparkException("ERROR: unknown model type: "+aModel.getClass());
        }

        return RDDUtils.addColumn(rowRDD.zip(predictions));
    }
}
