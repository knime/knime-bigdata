package com.knime.bigdata.spark.jobserver.jobs;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ModelUtils;
import com.knime.bigdata.spark.jobserver.server.NormalizationSettings;
import com.knime.bigdata.spark.jobserver.server.NormalizedRDDContainer;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

/**
 * @author dwk
 */
public class NormalizeColumnsJob extends KnimeSparkJob {

    private final static Logger LOGGER = Logger.getLogger(NormalizeColumnsJob.class.getName());

    private static final String PARAM_NORMALIZATION_COMPUTE_SETTINGS = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_STRING;

    private static final String PARAM_NORMALIZATION_APPLY_SETTINGS = ParameterConstants.PARAM_INPUT + "."
            + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_STRING, 1);

    /**
     * parse parameters
     *
     */
    @Override
    public SparkJobValidation validate(final Config aConfig) {
        String msg = null;
        if (!aConfig.hasPath(SupervisedLearnerUtils.PARAM_TRAINING_RDD)) {
            msg = "Input parameter '" + SupervisedLearnerUtils.PARAM_TRAINING_RDD + "' missing.";
        }
        if (msg == null) {
            if (!aConfig.hasPath(PARAM_NORMALIZATION_COMPUTE_SETTINGS) && !aConfig.hasPath(PARAM_NORMALIZATION_APPLY_SETTINGS)) {
                msg = "Exactly one normalization input parameter must be set. Either '" + PARAM_NORMALIZATION_COMPUTE_SETTINGS + "' or '"+PARAM_NORMALIZATION_APPLY_SETTINGS+"'.";
            } else if (aConfig.hasPath(PARAM_NORMALIZATION_COMPUTE_SETTINGS)) {
                try {
                    if (getNormalizationComputeSettings(aConfig) == null) {
                        msg = "Input parameter '" + PARAM_NORMALIZATION_COMPUTE_SETTINGS + "' missing.";
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    msg = "Input parameter '" + PARAM_NORMALIZATION_COMPUTE_SETTINGS + "' has an invalid value.";
                }
            } else if (aConfig.hasPath(PARAM_NORMALIZATION_APPLY_SETTINGS)) {
                try {
                    if (getNormalizationApplySettings(aConfig) == null) {
                        msg = "Input parameter '" + PARAM_NORMALIZATION_APPLY_SETTINGS + "' missing.";
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    msg = "Input parameter '" + PARAM_NORMALIZATION_APPLY_SETTINGS + "' has an invalid value.";
                }
            }
        }

        if (msg == null && !aConfig.hasPath(SupervisedLearnerUtils.PARAM_COL_IDXS)) {
            msg = "Input parameter '" + SupervisedLearnerUtils.PARAM_COL_IDXS + "' missing.";
        } else {
            try {
                aConfig.getIntList(SupervisedLearnerUtils.PARAM_COL_IDXS);
            } catch (ConfigException e) {
                e.printStackTrace();
                msg =
                    "Input parameter '" + SupervisedLearnerUtils.PARAM_COL_IDXS
                        + "' is not of expected type 'integer list'.";
            }
        }

        if (msg == null && !aConfig.hasPath(SupervisedLearnerUtils.PARAM_OUTPUT_DATA_PATH)) {
            msg = "Output parameter '" + SupervisedLearnerUtils.PARAM_OUTPUT_DATA_PATH + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * (public for unit testing)
     * @param aConfig
     * @return NormalizationSettings as decoded from base64 encoding
     */
    public static NormalizationSettings getNormalizationComputeSettings(final Config aConfig) {
        return (NormalizationSettings)ModelUtils.fromString(aConfig.getString(PARAM_NORMALIZATION_COMPUTE_SETTINGS));
    }

    /**
     * (public for unit testing)
     * @param aConfig
     * @return  Double[][]  as decoded from base64 encoding
     */
    public static Double[][] getNormalizationApplySettings(final Config aConfig) {
        return (Double[][])ModelUtils.fromString(aConfig.getString(PARAM_NORMALIZATION_APPLY_SETTINGS));
    }


    /**
     * run the actual job, the result is serialized back to the client
     *
     * @throws GenericKnimeSparkException
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final Config aConfig) throws GenericKnimeSparkException {
        SupervisedLearnerUtils.validateInput(aConfig, this, LOGGER);
        LOGGER.log(Level.INFO, "starting normalization job...");

        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getString(SupervisedLearnerUtils.PARAM_TRAINING_RDD));
        final NormalizedRDDContainer normalizedRowRDD = execute(aConfig, rowRDD);

        addToNamedRdds(aConfig.getString(SupervisedLearnerUtils.PARAM_OUTPUT_DATA_PATH), normalizedRowRDD.getRdd());

        JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(normalizedRowRDD);

        LOGGER.log(Level.INFO, "done");
        return res;
    }

    /**
     * (public for unit testing)
     * normalizes the given RDD according to config parameters
     * @param aConfig
     * @param aInputRowRDD
     * @return NormalizedRDDContainer normalization result
     */
    public static NormalizedRDDContainer execute(final Config aConfig, final JavaRDD<Row> aInputRowRDD) {
        final List<Integer> cols = aConfig.getIntList(SupervisedLearnerUtils.PARAM_COL_IDXS);

        final boolean isCompute = aConfig.hasPath(PARAM_NORMALIZATION_COMPUTE_SETTINGS);
        //normalize ALL given columns according to mode/parameters (for example, into 0-1 range)
        final NormalizedRDDContainer normalizedRowRDD;
        if (isCompute) {
            normalizedRowRDD = RDDUtilsInJava.normalize(aInputRowRDD, cols, getNormalizationComputeSettings(aConfig));
        } else {
            normalizedRowRDD = RDDUtilsInJava.normalize(aInputRowRDD, cols, getNormalizationApplySettings(aConfig));
        }
        return normalizedRowRDD;
    }

}