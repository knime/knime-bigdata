package com.knime.bigdata.spark.jobserver.jobs;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithm;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

import com.knime.bigdata.spark.jobserver.server.JobConfig;

import scala.Tuple2;

/**
 * @author dwk
 */
public class LinearRegressionWithSGDJob extends AbstractRegularizationJob {

    private final static Logger LOGGER = Logger.getLogger(LinearRegressionWithSGDJob.class.getName());

    /**
     * @param inputRdd
     * @return LinearRegressionModel
     */
    @Override
    public LinearRegressionModel execute(final SparkContext aContext, final JobConfig aConfig, final JavaRDD<LabeledPoint> inputRdd) {
        LinearRegressionModel model = configureLinRegWithSGD(aConfig).run(inputRdd.rdd().cache());
        //evaluateModel(inputRdd, model);
        return model;
    }

    static GeneralizedLinearAlgorithm<LinearRegressionModel> configureLinRegWithSGD(final JobConfig aConfig) {
        final LinearRegressionWithSGD alg = new LinearRegressionWithSGD();
        alg.setFeatureScaling(getFeatureScaling(aConfig)).setIntercept(getIntercept(aConfig))
        .setValidateData(getValidateData(aConfig));
        configureSGDOptimizer(aConfig, alg.optimizer());
        return alg;
    }

    /**
     * @param aData
     * @param aModel
     * @return Mean squared error
     */
    public static double evaluateModel(final JavaRDD<LabeledPoint> aData, final LinearRegressionModel aModel) {
        // Evaluate model on training examples and compute training error
        JavaRDD<Tuple2<Double, Double>> valuesAndPreds =
            aData.map(new Function<LabeledPoint, Tuple2<Double, Double>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<Double, Double> call(final LabeledPoint point) {
                    double prediction = aModel.predict(point.features());
                    return new Tuple2<Double, Double>(prediction, point.label());
                }
            });
        double MSE = new JavaDoubleRDD(valuesAndPreds.map(new Function<Tuple2<Double, Double>, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object call(final Tuple2<Double, Double> pair) {
                final double mse = Math.pow(pair._1() - pair._2(), 2.0);
                LOGGER.log(Level.SEVERE, "error: " + mse + " for value: " + pair._2() + " and prediction: " + pair._1());
                return mse;
            }
        }).rdd()).mean();
        LOGGER.log(Level.SEVERE, "training Mean Squared Error = " + MSE);
        return MSE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    Logger getLogger() {
        return LOGGER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String getAlgName() {
        return "Linear Regression With SGD";
    }
}