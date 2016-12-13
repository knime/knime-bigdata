package com.knime.bigdata.spark1_6.jobs.mllib.prediction.linear;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.optimization.Gradient;
import org.apache.spark.mllib.optimization.GradientDescent;
import org.apache.spark.mllib.optimization.HingeGradient;
import org.apache.spark.mllib.optimization.L1Updater;
import org.apache.spark.mllib.optimization.LeastSquaresGradient;
import org.apache.spark.mllib.optimization.LogisticGradient;
import org.apache.spark.mllib.optimization.SimpleUpdater;
import org.apache.spark.mllib.optimization.SquaredL2Updater;
import org.apache.spark.mllib.optimization.Updater;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.ModelJobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.EnumContainer.LinearLossFunction;
import com.knime.bigdata.spark.core.job.util.EnumContainer.LinearRegularizer;
import com.knime.bigdata.spark.node.mllib.prediction.linear.LinearLearnerJobInput;
import com.knime.bigdata.spark1_6.api.NamedObjects;
import com.knime.bigdata.spark1_6.api.SparkJob;
import com.knime.bigdata.spark1_6.api.SupervisedLearnerUtils;

/**
 * @author dwk
 * @param <I> {@link LinearLearnerJobInput} implementation
 */
@SparkClass
public abstract class AbstractRegularizationJob<I extends LinearLearnerJobInput>
implements SparkJob<I, ModelJobOutput> {

    private static final long serialVersionUID = 1L;

    /**
     * {@inheritDoc}
     */
    @Override
    public ModelJobOutput runJob(final SparkContext sparkContext, final I input,
        final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        getLogger().log(Level.INFO, "starting " + getAlgName() + " job...");

        //note that the column in the input RDD should be normalized into 0-1 ranges
        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());
        final JavaRDD<LabeledPoint> inputRdd = SupervisedLearnerUtils.getTrainingData(input, rowRDD);
        final Serializable model = execute(sparkContext, input, inputRdd);
        SupervisedLearnerUtils.storePredictions(sparkContext, namedObjects, input, rowRDD, inputRdd, model, getLogger());
        getLogger().log(Level.INFO, getAlgName() + " done");
        // note that with Spark 1.4 we can use PMML instead
        return new ModelJobOutput(model);
    }

    /**
     * @param aConfig
     * @param optimizer
     */
    protected void configureSGDOptimizer(final I aConfig, final GradientDescent optimizer) {
        optimizer.setNumIterations(aConfig.getNoOfIterations()).setRegParam(aConfig.getRegularization())
            .setUpdater(getUpdater(aConfig)).setGradient(getGradient(aConfig));
        optimizer.setMiniBatchFraction(aConfig.getFraction()).setStepSize(aConfig.getStepSize());
    }

    /**
     * @return the configured logger
     */
    protected abstract Logger getLogger();

    /**
     * @return name of this learner (primarily for logging)
     */
    protected abstract String getAlgName();

    /**
     * @param sc
     * @param aConfig
     * @param inputRdd
     * @return
     */
    protected abstract Serializable execute(final SparkContext sc, final I aConfig, final JavaRDD<LabeledPoint> inputRdd);


    /**
     * @param aConfig
     * @return
     */
    protected Gradient getGradient(final I aConfig) {
        //HingeGradient, LeastSquaresGradient, LogisticGradient
        final LinearLossFunction type = aConfig.getLossFunction();
        switch (type) {
            case Hinge:
                return new HingeGradient();
            case LeastSquares:
                return new LeastSquaresGradient();
            case Logistic:
                return new LogisticGradient();
            default:
                throw new IllegalArgumentException("Unsupported gradient type: " + type);
        }
    }

    /**
     * @param aConfig
     * @return
     */
    protected Updater getUpdater(final I aConfig) {
        // supported are: L1Updater, SimpleUpdater, SquaredL2Updater
        final LinearRegularizer updaterType = aConfig.getRegularizer();
        switch (updaterType) {
            case L1:
                return new L1Updater();
            case zero:
                return new SimpleUpdater();
            case L2:
                return new SquaredL2Updater();
            default:
                throw new IllegalArgumentException("Unsupported updated type: " + updaterType);
        }
    }
}