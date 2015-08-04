package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.optimization.L1Updater;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

import scala.Tuple2;

import com.typesafe.config.Config;

/**
 * @author dwk
 */
public class LinearRegressionWithSGDJob extends SGDJob {

    private final static Logger LOGGER = Logger.getLogger(LinearRegressionWithSGDJob.class.getName());

    /**
     * @param sc
     * @param aConfig
     * @param inputRdd
     * @return
     */
    @Override
    Serializable execute(final SparkContext sc, final Config aConfig, final JavaRDD<LabeledPoint> inputRdd) {
        return execute(inputRdd, aConfig.getInt(PARAM_NUM_ITERATIONS), aConfig.getDouble(PARAM_REGULARIZATION));
    }

    /**
     * (for unit testing)
     * @param inputRdd
     * @param aNoOfIterations
     * @param aRegParam
     * @return LinearRegressionModel
     */
    public LinearRegressionModel execute(final JavaRDD<LabeledPoint> inputRdd, final int aNoOfIterations, final double aRegParam) {
        final LinearRegressionWithSGD alg = new LinearRegressionWithSGD();
        alg.optimizer().setNumIterations(aNoOfIterations).setRegParam(aRegParam)
        .setUpdater(new L1Updater());
        LinearRegressionModel model = alg.run(inputRdd.rdd().cache());
        evaluateModel(inputRdd, model);
        return model;
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
                LOGGER.log(Level.SEVERE, "error: " + mse+ " for value: "+pair._2()+ " and prediction: "+pair._1());
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