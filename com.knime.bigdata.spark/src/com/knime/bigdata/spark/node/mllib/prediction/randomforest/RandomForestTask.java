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
package com.knime.bigdata.spark.node.mllib.prediction.randomforest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.AbstractTreeLearnerJob;
import com.knime.bigdata.spark.jobserver.jobs.RandomForestLearnerJob;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.FeatureSubsetStrategies;
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
public class RandomForestTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Integer[] m_featureColIdxs;

    private final int m_classColIdx;

    private final KNIMESparkContext m_context;

    private final String m_inputTableName;

    private final int m_maxDepth;

    private final int m_maxNoOfBins;

    private final String m_qualityMeasure;

    private final Long m_noOfClasses;

    private final NominalFeatureInfo m_nomFeatureInfo;

    private final FeatureSubsetStrategies m_featureSubsetStrategy;

    private final Integer m_numTrees;

    private final Boolean m_isClassification;

    private final Integer m_randomSeed;

    RandomForestTask(final SparkRDD inputRDD, final Integer[] featureColIdxs,
        final NominalFeatureInfo nominalFeatureInfo, final String classColName, final int classColIdx,
        final Long noOfClasses, final int maxDepth, final int maxNoOfBins, final String qualityMeasure,
        final int aNumTrees, final boolean aIsClassification, final FeatureSubsetStrategies aFSStrategy,
        final Integer aRandomSeed) {
        this(inputRDD.getContext(), inputRDD.getID(), featureColIdxs, nominalFeatureInfo, classColIdx, noOfClasses,
            maxDepth, maxNoOfBins, qualityMeasure, aNumTrees, aIsClassification, aFSStrategy, aRandomSeed);
    }

    RandomForestTask(final KNIMESparkContext aContext, final String aInputRDD, final Integer[] featureColIdxs,
        final NominalFeatureInfo nominalFeatureInfo, final int classColIdx, final Long noOfClasses, final int maxDepth,
        final int maxNoOfBins, final String qualityMeasure, final int aNumTrees, final boolean aIsClassification,
        final FeatureSubsetStrategies aFSStrategy, final Integer aRandomSeed) {
        m_maxDepth = maxDepth;
        m_maxNoOfBins = maxNoOfBins;
        m_qualityMeasure = qualityMeasure;
        m_context = aContext;
        m_inputTableName = aInputRDD;
        m_featureColIdxs = featureColIdxs;
        m_nomFeatureInfo = nominalFeatureInfo;
        m_classColIdx = classColIdx;
        m_noOfClasses = noOfClasses;
        m_numTrees = aNumTrees;
        m_isClassification = aIsClassification;
        m_featureSubsetStrategy = aFSStrategy;
        m_randomSeed = aRandomSeed;
    }

    RandomForestModel execute(final ExecutionMonitor exec) throws GenericKnimeSparkException,
        CanceledExecutionException {
        final String learnerParams = learnerDef();
        if (exec != null) {
            exec.checkCanceled();
        }
        final JobResult result =
            JobControler.startJobAndWaitForResult(m_context, RandomForestLearnerJob.class.getCanonicalName(),
                learnerParams, exec);

        return (RandomForestModel)result.getObjectResult();
    }

    /**
     * names of the columns (must include label column), required for value mapping info
     *
     * @throws GenericKnimeSparkException
     */
    String learnerDef() throws GenericKnimeSparkException {
        return paramsAsJason(m_inputTableName, m_featureColIdxs, m_nomFeatureInfo, m_classColIdx, m_noOfClasses,
            m_maxDepth, m_maxNoOfBins, m_qualityMeasure, m_numTrees, m_isClassification, m_featureSubsetStrategy,
            m_randomSeed);
    }

    /**
     * only values that are explicitly marked as Nullable are truly optional, the others are only checked for null so
     * that we can unit test the job validation
     *
     * @param aInputRDD
     * @param featureColIdxs
     * @param nominalFeatureInfo
     * @param classColIdx
     * @param noOfClasses
     * @param maxDepth
     * @param maxNoOfBins
     * @param qualityMeasure
     * @param aNumTrees
     * @param aIsClassification
     * @param aFSStrategy
     * @param aSeed
     * @return
     * @throws GenericKnimeSparkException
     */
    static String paramsAsJason(final String aInputRDD, final Integer[] featureColIdxs,
        @Nullable final NominalFeatureInfo nominalFeatureInfo, final Integer classColIdx,
        @Nullable final Long noOfClasses, final Integer maxDepth, final Integer maxNoOfBins,
        final String qualityMeasure, final Integer aNumTrees, final Boolean aIsClassification,
        final FeatureSubsetStrategies aFSStrategy, final Integer aSeed) throws GenericKnimeSparkException {
        final List<Object> inputParams = new ArrayList<>();
        inputParams.add(KnimeSparkJob.PARAM_INPUT_TABLE);
        inputParams.add(aInputRDD);
        if (nominalFeatureInfo != null) {
            inputParams.add(SupervisedLearnerUtils.PARAM_NOMINAL_FEATURE_INFO);
            inputParams.add(JobConfig.encodeToBase64(nominalFeatureInfo));
        }
        if (qualityMeasure != null) {
            inputParams.add(AbstractTreeLearnerJob.PARAM_INFORMATION_GAIN);
            inputParams.add(qualityMeasure);
        }
        if (maxNoOfBins != null) {
            inputParams.add(AbstractTreeLearnerJob.PARAM_MAX_BINS);
            inputParams.add(maxNoOfBins);
        }
        if (maxDepth != null) {
            inputParams.add(AbstractTreeLearnerJob.PARAM_MAX_DEPTH);
            inputParams.add(maxDepth);
        }
        if (classColIdx != null) {
            inputParams.add(ParameterConstants.PARAM_LABEL_INDEX);
            inputParams.add(classColIdx);
        }
        if (noOfClasses != null) {
            inputParams.add(AbstractTreeLearnerJob.PARAM_NO_OF_CLASSES);
            inputParams.add(noOfClasses);
        }
        if (featureColIdxs != null && featureColIdxs.length > 0) {
            inputParams.add(ParameterConstants.PARAM_COL_IDXS);
            inputParams.add(JsonUtils.toJsonArray((Object[])featureColIdxs));
        }
        if (aFSStrategy != null) {
            inputParams.add(RandomForestLearnerJob.PARAM_FEATURE_SUBSET_STRATEGY);
            inputParams.add(aFSStrategy.toString());
        }
        if (aNumTrees != null) {
            inputParams.add(RandomForestLearnerJob.PARAM_NUM_TREES);
            inputParams.add(aNumTrees);
        }
        if (aSeed != null) {
            inputParams.add(ParameterConstants.PARAM_SEED);
            inputParams.add(aSeed);
        }

        if (aIsClassification != null) {
            inputParams.add(RandomForestLearnerJob.PARAM_IS_CLASSIFICATION);
            inputParams.add(aIsClassification);
        }
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            inputParams.toArray(new Object[inputParams.size()]), ParameterConstants.PARAM_OUTPUT, new String[]{}});
    }

}
