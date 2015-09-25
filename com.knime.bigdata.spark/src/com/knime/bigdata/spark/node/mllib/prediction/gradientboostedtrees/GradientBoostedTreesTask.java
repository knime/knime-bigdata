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
package com.knime.bigdata.spark.node.mllib.prediction.gradientboostedtrees;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.spark.mllib.tree.configuration.Algo;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.configuration.Strategy;
import org.apache.spark.mllib.tree.impurity.Entropy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.GradientBoostedTreesLearnerJob;
import com.knime.bigdata.spark.jobserver.server.EnumContainer;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.LossFunctions;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.NominalFeatureInfo;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 *
 * @author koetter
 */
public class GradientBoostedTreesTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Integer[] m_featureColIdxs;

    private final int m_classColIdx;

    private final KNIMESparkContext m_context;

    private final String m_inputTableName;

    private final int m_maxDepth;

    private final int m_maxNoOfBins;

    private final Long m_noOfClasses;

    private final NominalFeatureInfo m_nomFeatureInfo;

    private final Integer m_numIterations;

    private final Boolean m_isClassification;

    //default 0.1
    private final double m_learningRate;

    //TODO - add as param
    private final LossFunctions m_lossFunction;

    //there are more options, seach for 'further options' below

    //note that as of Spark 1.2.1 only binary classification or regression is supported!

    //note that max depth must be <= 30 (at least in Spark 1.2.1)

    GradientBoostedTreesTask(final SparkRDD inputRDD, final Integer[] featureColIdxs,
        final NominalFeatureInfo nominalFeatureInfo, final String classColName, final int classColIdx,
        final Long noOfClasses, final int maxDepth, final int maxNoOfBins, final int aNumIterations,
        final double aLearningRate, final boolean aIsClassification) {
        this(inputRDD.getContext(), inputRDD.getID(), featureColIdxs, nominalFeatureInfo, classColIdx, noOfClasses,
            maxDepth, maxNoOfBins, aNumIterations, aLearningRate, aIsClassification);
    }

    GradientBoostedTreesTask(final KNIMESparkContext aContext, final String aInputRDD, final Integer[] featureColIdxs,
        final NominalFeatureInfo nominalFeatureInfo, final int classColIdx, final Long noOfClasses, final int maxDepth,
        final int maxNoOfBins, final int aNumIterations, final double aLearningRate, final boolean aIsClassification) {
        m_maxDepth = maxDepth;
        m_maxNoOfBins = maxNoOfBins;
        m_context = aContext;
        m_inputTableName = aInputRDD;
        m_featureColIdxs = featureColIdxs;
        m_nomFeatureInfo = nominalFeatureInfo;
        m_classColIdx = classColIdx;
        m_noOfClasses = noOfClasses;
        m_numIterations = aNumIterations;
        m_isClassification = aIsClassification;
        m_learningRate = aLearningRate;
        m_lossFunction =
            aIsClassification ? EnumContainer.LossFunctions.LogLoss : EnumContainer.LossFunctions.SquaredError;
    }

    GradientBoostedTreesModel execute(final ExecutionMonitor exec) throws GenericKnimeSparkException,
        CanceledExecutionException {
        final String learnerParams = learnerDef();
        if (exec != null) {
            exec.checkCanceled();
        }
        final JobResult result =
            JobControler.startJobAndWaitForResult(m_context, GradientBoostedTreesLearnerJob.class.getCanonicalName(),
                learnerParams, exec);

        return (GradientBoostedTreesModel)result.getObjectResult();
    }

    /**
     * names of the columns (must include label column), required for value mapping info
     *
     * @throws GenericKnimeSparkException
     */
    String learnerDef() throws GenericKnimeSparkException {
        return paramsAsJason(m_inputTableName, m_featureColIdxs, m_nomFeatureInfo, m_classColIdx, m_noOfClasses,
            m_maxDepth, m_maxNoOfBins, m_numIterations, m_learningRate, m_isClassification, m_lossFunction);
    }

    /**
     * only values that are explicitly marked as Nullable are truly optional, the others are only checked for null so
     * that we can unit test the job validation
     *
     * @throws GenericKnimeSparkException
     */
    static String paramsAsJason(final String aInputRDD, final Integer[] featureColIdxs,
        @Nullable final NominalFeatureInfo nominalFeatureInfo, final Integer classColIdx,
        @Nullable final Long aNrOfClasses, final int maxDepth, final int maxBins, final Integer aNumIterations,
        final double aLearningRate, final boolean aIsClassification, final EnumContainer.LossFunctions aLossFunction)
        throws GenericKnimeSparkException {
        final List<Object> inputParams = new ArrayList<>();
        inputParams.add(KnimeSparkJob.PARAM_INPUT_TABLE);
        inputParams.add(aInputRDD);

        if (classColIdx != null) {
            inputParams.add(ParameterConstants.PARAM_LABEL_INDEX);
            inputParams.add(classColIdx);
        }
        final int nrOfClasses;
        if (aNrOfClasses != null) {
            inputParams.add(SupervisedLearnerUtils.PARAM_NO_OF_CLASSES);
            inputParams.add(aNrOfClasses);
            nrOfClasses = aNrOfClasses.intValue();
        } else {
            //number of classes will be computed by back-end
            nrOfClasses = 2;
        }
        if (featureColIdxs != null && featureColIdxs.length > 0) {
            inputParams.add(ParameterConstants.PARAM_COL_IDXS);
            inputParams.add(JsonUtils.toJsonArray((Object[])featureColIdxs));
        }

        if (aLossFunction != null) {
            inputParams.add(GradientBoostedTreesLearnerJob.PARAM_LOSS_FUNCTION);
            inputParams.add(aLossFunction.toString());
        }

        // treeStrategy - Parameters for the tree algorithm. We support regression and binary classification for boosting. Impurity setting will be ignored.
        // loss - Loss function used for minimization during gradient boosting.
        // numIterations -    Number of iterations of boosting. In other words, the number of weak hypotheses used in the final model.
        //learningRate -   Learning rate for shrinking the contribution of each estimator. The learning rate should be between in the interval (0, 1]

        final Map<Integer, Integer> categoricalFeatureInfo = new HashMap<>();
        if (nominalFeatureInfo != null) {
            categoricalFeatureInfo.putAll(nominalFeatureInfo.getMap());
        }

        final Strategy treeStrategy;
        if (aIsClassification) {
            treeStrategy =
                new Strategy(Algo.Classification(), Entropy.instance(), maxDepth, nrOfClasses, maxBins,
                    categoricalFeatureInfo);
        } else {
            treeStrategy =
                new Strategy(Algo.Regression(), Entropy.instance(), maxDepth, nrOfClasses, maxBins,
                    categoricalFeatureInfo);
        }

        //further options (default values are shown):
        //final Option<String> checkpointDir = scala.None;
        //treeStrategy.setCheckpointDir(checkpointDir);
        //final int checkpointInterval = 10;
        //treeStrategy.setCheckpointInterval(checkpointInterval);

        //Map<Integer, Integer> quantileCalculationStrategy = new HashMap<>();
        // quantileCalculationStrategy: QuantileStrategy.QuantileStrategy = ..., categoricalFeaturesInfo: Map[Int, Int] = ...,
        //treeStrategy.setQuantileCalculationStrategy(quantileCalculationStrategy);

        //        final int minInstancesPerNode = 1;
        //        treeStrategy.setMinInstancesPerNode(minInstancesPerNode);
        //        final double minInfoGain = 0.0;
        //        treeStrategy.setMinInfoGain(minInfoGain);
        //        final int maxMemoryInMB = 256;
        //        treeStrategy.setMaxMemoryInMB(maxMemoryInMB);
        //        final double subsamplingRate = 1d;
        //        treeStrategy.setSubsamplingRate(subsamplingRate);
        //        final boolean useNodeIdCache = false;
        //        treeStrategy.setUseNodeIdCache(useNodeIdCache);

        //impurity setting is ignored by GBT
        //treeStrategy.setImpurity(IGNORE);

        final BoostingStrategy boostingStrategy =
            new BoostingStrategy(treeStrategy, null, aNumIterations, aLearningRate);

        inputParams.add(GradientBoostedTreesLearnerJob.PARAM_BOOSTING_STRATEGY);
        inputParams.add(JobConfig.encodeToBase64(boostingStrategy));
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            inputParams.toArray(new Object[inputParams.size()]), ParameterConstants.PARAM_OUTPUT, new String[]{}});
    }
}
