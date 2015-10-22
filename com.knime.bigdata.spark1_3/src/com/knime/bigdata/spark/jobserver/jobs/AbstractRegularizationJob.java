package com.knime.bigdata.spark.jobserver.jobs;

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
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.EnumContainer.LinearLossFunctionTypeType;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.LinearRegularizerType;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * @author dwk
 */
public abstract class AbstractRegularizationJob extends KnimeSparkJob {

    /**
     * number of optimization iterations
     */
    public static final String PARAM_NUM_ITERATIONS = ParameterConstants.PARAM_NUM_ITERATIONS;

    /**
     * regularization parameter, should be some float between 0 and 1 (0.1)
     */
    public static final String PARAM_REGULARIZATION = "Regularization";

    /**
     * used to perform steps (weight update) using Gradient Descent methods.
     */
    public static final String PARAM_UPDATER_TYPE = "updaterType";

    /**
     * gradient descent type
     */
    public static final String PARAM_GRADIENT_TYPE = "gradientType";

    /**
     * (SGD only)
     */
    public static final String PARAM_STEP_SIZE = "stepSize";

    /**
     * (SGD only)
     */
    public static final String PARAM_FRACTION = "fraction";

    /**
     *  should the algorithm validate data before training?
     */
    public static final String PARAM_VALIDATE_DATA = "validateData";

    /**
     * should algorithm  add an intercept?
     */
    public static final String PARAM_ADD_INTERCEPT = "addIntercept";

    /**
     *
     */
    public static final String PARAM_USE_FEATURE_SCALING = "useFeatureScaling";

    /**
     * parse parameters
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;
        if (!aConfig.hasInputParameter(PARAM_NUM_ITERATIONS)) {
            msg = "Input parameter '" + PARAM_NUM_ITERATIONS + "' missing.";
        } else {
            try {
                getNumIterations(aConfig);
            } catch (Exception e) {
                msg = "Input parameter '" + PARAM_NUM_ITERATIONS + "' is not of expected type 'integer'.";
            }
        }
        if (msg == null) {
            if (!aConfig.hasInputParameter(PARAM_REGULARIZATION)) {
                msg = "Input parameter '" + PARAM_REGULARIZATION + "' missing.";
            } else {
                try {
                    getRegularization(aConfig);
                } catch (Exception e) {
                    msg = "Input parameter '" + PARAM_REGULARIZATION + "' is not of expected type 'double'.";
                }
            }
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_VALIDATE_DATA)) {
            msg = "Input parameter '" + PARAM_VALIDATE_DATA + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_ADD_INTERCEPT)) {
            msg = "Input parameter '" + PARAM_ADD_INTERCEPT + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_USE_FEATURE_SCALING)) {
            msg = "Input parameter '" + PARAM_USE_FEATURE_SCALING + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_UPDATER_TYPE)) {
            msg = "Input parameter '" + PARAM_UPDATER_TYPE + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_GRADIENT_TYPE)) {
            msg = "Input parameter '" + PARAM_GRADIENT_TYPE + "' missing.";
        }

        if (msg == null) {
            msg = SupervisedLearnerUtils.checkConfig(aConfig);
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * @param aConfig
     */
    static Double getRegularization(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_REGULARIZATION, Double.class);
    }

    static Integer getNumIterations(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_NUM_ITERATIONS, Integer.class);
    }

    /**
     * run the actual job, the result is serialized back to the client
     *
     * @throws GenericKnimeSparkException
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        SupervisedLearnerUtils.validateInput(aConfig, this, getLogger());
        getLogger().log(Level.INFO, "starting " + getAlgName() + " job...");

        //note that the column in the input RDD should be normalized into 0-1 ranges
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));

        final JavaRDD<LabeledPoint> inputRdd = SupervisedLearnerUtils.getTrainingData(aConfig, rowRDD);

        final Serializable model = execute(sc, aConfig, inputRdd);

        JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(model);

        if (aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            SupervisedLearnerUtils.storePredictions(sc, aConfig, this, rowRDD,
                RDDUtils.toVectorRDDFromLabeledPointRDD(inputRdd), model, getLogger());
        }

        getLogger().log(Level.INFO, getAlgName() + " done");
        // note that with Spark 1.4 we can use PMML instead
        return res;
    }

    /**
     * @return the configured logger
     */
    abstract Logger getLogger();

    /**
     * @return name of this learner (primarily for logging)
     */
    abstract String getAlgName();

    /**
     * @param sc
     * @param aConfig
     * @param inputRdd
     * @return
     */
    abstract Serializable execute(final SparkContext sc, final JobConfig aConfig, final JavaRDD<LabeledPoint> inputRdd);

    /**
     * @param aConfig
     * @param optimizer
     */
    static void configureSGDOptimizer(final JobConfig aConfig, final GradientDescent optimizer) {
        optimizer.setNumIterations(getNumIterations(aConfig)).setRegParam(getRegularization(aConfig))
            .setUpdater(getUpdater(aConfig)).setGradient(getGradient(aConfig));
        optimizer.setMiniBatchFraction(getFraction(aConfig)).setStepSize(getStepSize(aConfig));
    }

    /**
     * @param aConfig
     * @return
     */
    static double getStepSize(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_STEP_SIZE, Double.class);
    }

    /**
     * @param aConfig
     * @return
     */
    static double getFraction(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_FRACTION, Double.class);
    }

    /**
     * @param aConfig
     * @return
     */
    static Gradient getGradient(final JobConfig aConfig) {
        //HingeGradient, LeastSquaresGradient, LogisticGradient
        final LinearLossFunctionTypeType type = LinearLossFunctionTypeType.fromKnimeEnum(aConfig.getInputParameter(PARAM_GRADIENT_TYPE));
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
    static Updater getUpdater(final JobConfig aConfig) {
        // supported are: L1Updater, SimpleUpdater, SquaredL2Updater
        final LinearRegularizerType updaterType = LinearRegularizerType.fromKnimeEnum(aConfig.getInputParameter(PARAM_UPDATER_TYPE));
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

    /**
     * @param aConfig
     * @return
     */
    static Boolean getValidateData(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_VALIDATE_DATA, Boolean.class);
    }

    /**
     * @param aConfig
     * @return
     */
    static boolean getIntercept(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_ADD_INTERCEPT, Boolean.class);
    }

    /**
     * @param aConfig
     * @return
     */
    static boolean getFeatureScaling(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_USE_FEATURE_SCALING, Boolean.class);
    }

}