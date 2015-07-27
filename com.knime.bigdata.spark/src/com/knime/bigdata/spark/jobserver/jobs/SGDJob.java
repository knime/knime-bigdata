package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

/**
 * @author dwk
 */
public abstract class SGDJob extends KnimeSparkJob {

    protected static final String PARAM_NUM_ITERATIONS = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_NUM_ITERATIONS;

    /**
     * regularization parameter, should be some float between 0 and 1 (0.1)
     */
    protected static final String PARAM_REGULARIZATION = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_STRING;

    /**
     * parse parameters
     *
     */
    @Override
    public SparkJobValidation validate(final Config aConfig) {
        String msg = null;
        if (!aConfig.hasPath(PARAM_NUM_ITERATIONS)) {
            msg = "Input parameter '" + PARAM_NUM_ITERATIONS + "' missing.";
        } else {
            try {
                aConfig.getInt(PARAM_NUM_ITERATIONS);
            } catch (ConfigException e) {
                msg = "Input parameter '" + PARAM_NUM_ITERATIONS + "' is not of expected type 'integer'.";
            }
        }
        if (msg == null) {
            if (!aConfig.hasPath(PARAM_REGULARIZATION)) {
                msg = "Input parameter '" + PARAM_REGULARIZATION + "' missing.";
            } else {
                try {
                    aConfig.getDouble(PARAM_REGULARIZATION);
                } catch (ConfigException e) {
                    msg = "Input parameter '" + PARAM_REGULARIZATION + "' is not of expected type 'double'.";
                }
            }
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
     * run the actual job, the result is serialized back to the client
     *
     * @throws GenericKnimeSparkException
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final Config aConfig) throws GenericKnimeSparkException {
        SupervisedLearnerUtils.validateInput(aConfig, this, getLogger());
        getLogger().log(Level.INFO, "starting "+getAlgName()+" job...");

        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getString(SupervisedLearnerUtils.PARAM_TRAINING_RDD));
        final JavaRDD<LabeledPoint> inputRdd = SupervisedLearnerUtils.getTrainingData(aConfig, rowRDD);

        final Serializable model = execute(sc, aConfig, inputRdd);

        JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(model);

        if (aConfig.hasPath(SupervisedLearnerUtils.PARAM_OUTPUT_DATA_PATH)) {
            SupervisedLearnerUtils.storePredictions(sc, aConfig, this, rowRDD,
                RDDUtils.toVectorRDDFromLabeledPointRDD(inputRdd), model, getLogger());
        }

        getLogger().log(Level.INFO, getAlgName()+" done");
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
    abstract Serializable execute(final SparkContext sc, final Config aConfig, final JavaRDD<LabeledPoint> inputRdd);
}