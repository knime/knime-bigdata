/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.loss.AbsoluteError;
import org.apache.spark.mllib.tree.loss.LogLoss;
import org.apache.spark.mllib.tree.loss.Loss;
import org.apache.spark.mllib.tree.loss.SquaredError;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.tree.model.TreeEnsembleModel;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.EnumContainer;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.LossFunctions;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * runs MLlib GradientBoostedTrees on a given RDD to create a forest, model is returned as result
 *
 * @author koetter, dwk
 */
public class GradientBoostedTreesLearnerJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(GradientBoostedTreesLearnerJob.class.getName());

    /**
     * container with all algorithmic settings
     */
    public static final String PARAM_BOOSTING_STRATEGY = "boostingStrategy";

    /**
     * loss function - either AbsoluteError, LogLoss, or SquaredError
     */
    public static final String PARAM_LOSS_FUNCTION = "lossFunction";

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;

        if (!aConfig.hasInputParameter(PARAM_BOOSTING_STRATEGY)) {
            msg = "Input parameter '" + PARAM_BOOSTING_STRATEGY + "' missing.";
        }

        if (!aConfig.hasInputParameter(PARAM_LOSS_FUNCTION)) {
            msg = "Input parameter '" + PARAM_LOSS_FUNCTION + "' missing.";
        }

        if (msg == null) {
            msg = SupervisedLearnerUtils.checkConfig(aConfig);
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    static BoostingStrategy getBoostingStrategy(final JobConfig aConfig) throws GenericKnimeSparkException {
        return aConfig.decodeFromInputParameter(PARAM_BOOSTING_STRATEGY);
    }

    /**
     * run the actual job, the result is serialized back to the client
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        SupervisedLearnerUtils.validateInput(aConfig, this, LOGGER);
        LOGGER.log(Level.INFO, "starting 'Gradient Boosted Trees' learner job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        final JavaRDD<LabeledPoint> inputRdd = SupervisedLearnerUtils.getTrainingData(aConfig, rowRDD);
        final GradientBoostedTreesModel model = execute(aConfig, inputRdd);

        JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(model);

        if (aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            SupervisedLearnerUtils.storePredictions(sc, aConfig, this, rowRDD,
                RDDUtils.toVectorRDDFromLabeledPointRDD(inputRdd), model, LOGGER);
        }
        LOGGER.log(Level.INFO, "'Gradient Boosted Trees' done");
        // note that with Spark 1.4 we can use PMML instead
        return res;

    }

    /**
     * Train a GradientBoostedTrees model.
     *
     * @param aConfig
     * @param aInputData - Training dataset: RDD of LabeledPoint.
     * @return GradientBoostedTreesModel
     * @throws GenericKnimeSparkException
     */
    private static GradientBoostedTreesModel execute(final JobConfig aConfig, final JavaRDD<LabeledPoint> aInputData)
        throws GenericKnimeSparkException {
        aInputData.cache();
        final BoostingStrategy boostingStrategy = getBoostingStrategy(aConfig);
        if (boostingStrategy.getTreeStrategy().getAlgo().toString().equals("Classification")) {
            final Integer labelIndex = aConfig.getInputParameter(ParameterConstants.PARAM_LABEL_INDEX, Integer.class);
            final Long numClasses;
            if (aConfig.hasInputParameter(SupervisedLearnerUtils.PARAM_NO_OF_CLASSES)) {
                numClasses = aConfig.getInputParameter(SupervisedLearnerUtils.PARAM_NO_OF_CLASSES, Long.class);
            } else if (boostingStrategy.getTreeStrategy().categoricalFeaturesInfo().contains(labelIndex)) {
                numClasses =
                    ((Number)boostingStrategy.getTreeStrategy().categoricalFeaturesInfo().get(labelIndex).get())
                        .longValue();
            } else {
                //Get number of classes from the input data
                numClasses = SupervisedLearnerUtils.getNumberOfLabels(aInputData);
            }
            boostingStrategy.getTreeStrategy().setNumClasses(numClasses.intValue());
            LOGGER.log(Level.INFO, "Training GBT for " + numClasses + " classes.");
        } else {
            LOGGER.log(Level.INFO, "Training Regression GBT.");
        }

        LossFunctions lossFunction = EnumContainer.LossFunctions.fromKnimeEnum(aConfig
            .getInputParameter(PARAM_LOSS_FUNCTION));
        LOGGER.log(Level.INFO, "GBT loss function: "+lossFunction.toString());
        boostingStrategy.setLoss(LossFactory.getLossFunction(lossFunction));

        return GradientBoostedTrees.train(aInputData, boostingStrategy);
    }

    private static class LossFactory {
        static Loss getLossFunction(final EnumContainer.LossFunctions aLossFunction) {
            //one of     AbsoluteError, LogLoss, SquaredError
            switch (aLossFunction) {
                case LogLoss:
                    return new Loss() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public double gradient(final TreeEnsembleModel arg0, final LabeledPoint arg1) {
                            return LogLoss.gradient(arg0, arg1);
                        }

                        @Override
                        public double computeError(final TreeEnsembleModel arg0, final RDD<LabeledPoint> arg1) {
                            return LogLoss.computeError(arg0, arg1);
                        }
                    };
                case SquaredError:
                    return new Loss() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public double gradient(final TreeEnsembleModel arg0, final LabeledPoint arg1) {
                            return SquaredError.gradient(arg0, arg1);
                        }

                        @Override
                        public double computeError(final TreeEnsembleModel arg0, final RDD<LabeledPoint> arg1) {
                            return SquaredError.computeError(arg0, arg1);
                        }
                    };
                case AbsoluteError:
                    return new Loss() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public double gradient(final TreeEnsembleModel arg0, final LabeledPoint arg1) {
                            return AbsoluteError.gradient(arg0, arg1);
                        }

                        @Override
                        public double computeError(final TreeEnsembleModel arg0, final RDD<LabeledPoint> arg1) {
                            return AbsoluteError.computeError(arg0, arg1);
                        }
                    };
                    default:
                        throw new IllegalArgumentException("Unsupported loss function: "+aLossFunction.toString());
            }
        }
    }

}
