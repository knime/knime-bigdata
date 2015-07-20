package com.knime.bigdata.spark.jobserver.jobs;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.optimization.L1Updater;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ModelUtils;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.InvalidSchemaException;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

/**
 * @author Tobias Koetter, KNIME.com
 */
public class SVMLearnerJob extends KnimeSparkJob {

    private final static Logger LOGGER = Logger.getLogger(SVMLearnerJob.class.getName());

    private static final String PARAM_NUM_ITERATIONS = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_NUM_ITERATIONS;

    private static final String PARAM_CLASS_COL_IDX = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_CLASS_COL_IDX;

    private static final String PARAM_COL_IDXS = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_COL_IDXS;

    private static final String PARAM_DATA_FILE_NAME = ParameterConstants.PARAM_INPUT + "."
        + ParameterConstants.PARAM_TABLE_1;

    private static final String PARAM_OUTPUT_DATA_PATH = ParameterConstants.PARAM_OUTPUT + "."
        + ParameterConstants.PARAM_TABLE_1;

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
            if (!aConfig.hasPath(PARAM_CLASS_COL_IDX)) {
                msg = "Input parameter '" + PARAM_CLASS_COL_IDX + "' missing.";
            } else {
                try {
                    aConfig.getInt(PARAM_CLASS_COL_IDX);
                } catch (ConfigException e) {
                    msg = "Input parameter '" + PARAM_CLASS_COL_IDX + "' is not of expected type 'integer'.";
                }
            }
        }

        if (msg == null) {
            if (!aConfig.hasPath(PARAM_COL_IDXS)) {
                msg = "Input parameter '" + PARAM_COL_IDXS + "' missing.";
            } else {
                try {
                    aConfig.getIntList(PARAM_COL_IDXS);
                } catch (ConfigException e) {
                    msg = "Input parameter '" + PARAM_COL_IDXS + "' is not of expected type 'integer list'.";
                }
            }
        }

        if (msg == null && !aConfig.hasPath(PARAM_DATA_FILE_NAME)) {
            msg = "Input parameter '" + PARAM_DATA_FILE_NAME + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    private void validateInput(final Config aConfig) throws GenericKnimeSparkException {
        String msg = null;
        final String key = aConfig.getString(PARAM_DATA_FILE_NAME);
        if (key == null) {
            msg = "Input parameter at port 1 is missing!";
        } else if (!validateNamedRdd(key)) {
            msg = "Input data table missing!";
        }
        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
        }
    }

    /**
     * run the actual job, the result is serialized back to the client
     *
     * @throws GenericKnimeSparkException
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final Config aConfig) throws GenericKnimeSparkException {
        validateInput(aConfig);
        LOGGER.log(Level.INFO, "starting kMeans job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getString(PARAM_DATA_FILE_NAME));
        List<Row> collect = rowRDD.collect();
        for (Row r : collect) {
            RowBuilder builder = RowBuilder.emptyRow();
            for (int i = 0; i < r.length(); i++) {
                final String val = r.getString(i);
                try {
                    builder.add(new Integer(Integer.parseInt(val)));
                } catch (Exception e) {
                    builder.add(val);
                }
            }
            return builder.build();
        }
        final int classColIdx = aConfig.getInt(PARAM_CLASS_COL_IDX);
        final List<Integer> colIdxs = aConfig.getIntList(PARAM_COL_IDXS);
        final int noOfIteration = aConfig.getInt(PARAM_NUM_ITERATIONS);
        //use only the column indices when converting to vector
        final JavaRDD<LabeledPoint> inputRDD = RDDUtils.toJavaLabeledPointRDD(rowRDD, classColIdx, colIdxs);

        final SVMWithSGD svmAlg = new SVMWithSGD();
        svmAlg.optimizer().setNumIterations(noOfIteration).setRegParam(0.1).setUpdater(new L1Updater());
        final SVMModel model = svmAlg.run(inputRDD);
        JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(model);

        if (aConfig.hasPath(PARAM_OUTPUT_DATA_PATH)) {
            LOGGER.log(Level.INFO, "Storing predicted data unter key: " + aConfig.getString(PARAM_OUTPUT_DATA_PATH));
            JavaRDD<Row> predictedData = ModelUtils.predict(sc, inputRDD, rowRDD, model);
            try {
                addToNamedRdds(aConfig.getString(PARAM_OUTPUT_DATA_PATH), predictedData);
                try {
                    final StructType schema = StructTypeBuilder.fromRows(predictedData.take(10)).build();
                    res = res.withTable(aConfig.getString(PARAM_DATA_FILE_NAME), schema);
                } catch (InvalidSchemaException e) {
                    return JobResult.emptyJobResult().withMessage("ERROR: " + e.getMessage());
                }
            } catch (Exception e) {
                LOGGER.severe("ERROR: failed to predict and store results for training data.");
                LOGGER.severe(e.getMessage());
            }
        }
        LOGGER.log(Level.INFO, "kMeans done");
        // note that with Spark 1.4 we can use PMML instead
        return res;
    }
}