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
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.AbstractTreeLearnerJob;
import com.knime.bigdata.spark.jobserver.jobs.DecisionTreeLearnerJob;
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
public class DecisionTreeTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer[] m_featureColIdxs;

    private final int m_classColIdx;

    private final KNIMESparkContext m_context;

    private String m_inputTableName;

    //private final String[] m_colNames;

    private int m_maxDepth;

    private int m_maxNoOfBins;

    private String m_qualityMeasure;

    private Long m_noOfClasses;

    private NominalFeatureInfo m_nomFeatureInfo;

    DecisionTreeTask(final SparkRDD inputRDD, final Integer[] featureColIdxs, final List<String> afeatureColNames,
        final NominalFeatureInfo nominalFeatureInfo, final String classColName, final int classColIdx,
        final Long noOfClasses, final int maxDepth, final int maxNoOfBins, final String qualityMeasure) {
        this(inputRDD.getContext(), inputRDD.getID(), featureColIdxs, nominalFeatureInfo, classColIdx, noOfClasses,
            maxDepth, maxNoOfBins, qualityMeasure);
    }

    DecisionTreeTask(final KNIMESparkContext aContext, final String aInputRDD, final Integer[] featureColIdxs,
        final NominalFeatureInfo nominalFeatureInfo, final int classColIdx, final Long noOfClasses, final int maxDepth,
        final int maxNoOfBins, final String qualityMeasure) {
        m_maxDepth = maxDepth;
        m_maxNoOfBins = maxNoOfBins;
        m_qualityMeasure = qualityMeasure;
        m_context = aContext;
        m_inputTableName = aInputRDD;
        m_featureColIdxs = featureColIdxs;
        m_nomFeatureInfo = nominalFeatureInfo;
        m_classColIdx = classColIdx;
        m_noOfClasses = noOfClasses;
        //        final List<String> allColNames = new LinkedList<>(afeatureColNames);
        //        allColNames.add(classColName);
        //        m_colNames = allColNames.toArray(new String[allColNames.size()]);
    }

    DecisionTreeModel execute(final ExecutionMonitor exec) throws GenericKnimeSparkException,
        CanceledExecutionException {
        final String learnerParams = learnerDef();
        if (exec != null) {
            exec.checkCanceled();
        }
        final JobResult result =
            JobControler.startJobAndWaitForResult(m_context, DecisionTreeLearnerJob.class.getCanonicalName(),
                learnerParams, exec);

        return (DecisionTreeModel)result.getObjectResult();
    }

    /**
     * names of the columns (must include label column), required for value mapping info
     *
     * @throws GenericKnimeSparkException
     */
    String learnerDef() throws GenericKnimeSparkException {
        return paramsAsJason(m_inputTableName, m_featureColIdxs, m_nomFeatureInfo, m_classColIdx, m_noOfClasses,
            m_maxDepth, m_maxNoOfBins, m_qualityMeasure);
    }

    static String paramsAsJason(final String aInputRDD, final Integer[] featureColIdxs,
        final NominalFeatureInfo nominalFeatureInfo, final int classColIdx, final Long noOfClasses, final int maxDepth,
        final int maxNoOfBins, final String qualityMeasure) throws GenericKnimeSparkException {
        final List<Object> inputParams = new ArrayList<>();
        inputParams.add(KnimeSparkJob.PARAM_INPUT_TABLE);
        inputParams.add(aInputRDD);
        if (nominalFeatureInfo != null) {
            inputParams.add(SupervisedLearnerUtils.PARAM_NOMINAL_FEATURE_INFO);
            inputParams.add(JobConfig.encodeToBase64(nominalFeatureInfo));
        }
        inputParams.add(AbstractTreeLearnerJob.PARAM_INFORMATION_GAIN);
        inputParams.add(qualityMeasure);
        inputParams.add(AbstractTreeLearnerJob.PARAM_MAX_BINS);
        inputParams.add(maxNoOfBins);
        inputParams.add(AbstractTreeLearnerJob.PARAM_MAX_DEPTH);
        inputParams.add(maxDepth);
        inputParams.add(ParameterConstants.PARAM_LABEL_INDEX);
        inputParams.add(classColIdx);
        if (noOfClasses != null) {
            inputParams.add(SupervisedLearnerUtils.PARAM_NO_OF_CLASSES);
            inputParams.add(noOfClasses);
        }
        //        inputParams.add(ParameterConstants.PARAM_COL_NAMES);
        //        inputParams.add(JsonUtils.toJsonArray((Object[])m_colNames));
        inputParams.add(ParameterConstants.PARAM_COL_IDXS);
        inputParams.add(JsonUtils.toJsonArray((Object[])featureColIdxs));
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            inputParams.toArray(new Object[inputParams.size()]), ParameterConstants.PARAM_OUTPUT, new String[]{}});
    }

}
