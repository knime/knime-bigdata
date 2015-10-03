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
package com.knime.bigdata.spark.node.mllib.prediction.linear;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.AbstractRegularizationJob;
import com.knime.bigdata.spark.jobserver.jobs.LogisticRegressionJob;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.GradientType;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.UpdaterType;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 * @author koetter, dwk
 */
public class LinearLearnerTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Integer[] m_featureColIdxs;

    private final int m_classColIdx;

    private final KNIMESparkContext m_context;

    private final String m_inputTableName;

    private final Integer m_numCorrections;

    private final Double m_tolerance;

    private final Integer m_numIterations;

    private final Boolean m_useSGD;

    private final double m_regularization;

    private final UpdaterType m_UpdaterType;

    private final Boolean m_ValidateData;

    private final Boolean m_AddIntercept;

    private final Boolean m_UseFeatureScaling;

    private final GradientType m_GradientType;

    private final Double m_StepSize;

    private final Double m_Fraction;

    private final String m_jobClassPath;

    /**
     * create a linear learner task that uses SGD
     * @param inputRDD
     * @param featureColIdxs
     * @param classColIdx
     * @param aNumIterations
     * @param aRegularization
     * @param aUpdaterType
     * @param aValidateData
     * @param aAddIntercept
     * @param aUseFeatureScaling
     * @param aGradientType
     * @param aStepSize
     * @param aFraction
     */
    LinearLearnerTask(final SparkRDD inputRDD, final Integer[] featureColIdxs, final int classColIdx,
        final int aNumIterations, final double aRegularization,
        final UpdaterType aUpdaterType,
        final Boolean aValidateData, final Boolean aAddIntercept, final Boolean aUseFeatureScaling,
        final GradientType aGradientType, final Double aStepSize,final Double aFraction, final Class<? extends AbstractRegularizationJob> jobClass) {
        this(inputRDD.getContext(), inputRDD.getID(), featureColIdxs, classColIdx, aNumIterations, aRegularization,
            true, null, null, aUpdaterType, aValidateData, aAddIntercept, aUseFeatureScaling, aGradientType, aStepSize, aFraction, jobClass);
    }

    /**
     * create a linear learner task that uses LBFGS
     * @param inputRDD
     * @param featureColIdxs
     * @param classColIdx
     * @param aNumIterations
     * @param aRegularization
     * @param aNumCorrections
     * @param aTolerance
     * @param aUpdaterType
     * @param aValidateData
     * @param aAddIntercept
     * @param aUseFeatureScaling
     * @param aGradientType
     */
    LinearLearnerTask(final SparkRDD inputRDD, final Integer[] featureColIdxs, final int classColIdx,
        final int aNumIterations, final double aRegularization,
        final Integer aNumCorrections, final Double aTolerance, final UpdaterType aUpdaterType,
        final Boolean aValidateData, final Boolean aAddIntercept, final Boolean aUseFeatureScaling,
        final GradientType aGradientType, final Class<? extends AbstractRegularizationJob> jobClass) {
        this(inputRDD.getContext(), inputRDD.getID(), featureColIdxs, classColIdx, aNumIterations, aRegularization,
            false, aNumCorrections, aTolerance, aUpdaterType, aValidateData, aAddIntercept, aUseFeatureScaling, aGradientType, null, null, jobClass);
    }

    //unit testing constructor only
    LinearLearnerTask(final KNIMESparkContext aContext, final String aInputRDD, final Integer[] featureColIdxs,
        final int classColIdx, final int aNumIterations, final double aRegularization, final boolean aUseSGD,
        @Nullable final Integer aNumCorrections, @Nullable final Double aTolerance, final UpdaterType aUpdaterType,
        final Boolean aValidateData, final Boolean aAddIntercept, final Boolean aUseFeatureScaling,
        final GradientType aGradientType, @Nullable final Double aStepSize, @Nullable final Double aFraction, final Class<? extends AbstractRegularizationJob> jobClass) {
        m_numCorrections = aNumCorrections;
        m_tolerance = aTolerance;
        m_context = aContext;
        m_inputTableName = aInputRDD;
        m_featureColIdxs = featureColIdxs;
        m_classColIdx = classColIdx;
        m_numIterations = aNumIterations;
        m_useSGD = aUseSGD;
        m_regularization = aRegularization;
        m_UpdaterType = aUpdaterType;
        m_ValidateData = aValidateData;
        m_AddIntercept = aAddIntercept;
        m_UseFeatureScaling = aUseFeatureScaling;
        m_GradientType = aGradientType;
        m_StepSize = aStepSize;
        m_Fraction = aFraction;
        m_jobClassPath = jobClass.getCanonicalName();
    }

    Serializable execute(final ExecutionMonitor exec) throws GenericKnimeSparkException,
        CanceledExecutionException {
        final String learnerParams = learnerDef();
        if (exec != null) {
            exec.checkCanceled();
        }
        final JobResult result = JobControler.startJobAndWaitForResult(m_context, m_jobClassPath, learnerParams, exec);
        return (Serializable)result.getObjectResult();
    }

    /**
     * names of the columns (must include label column), required for value mapping info
     *
     * @throws GenericKnimeSparkException
     */
    String learnerDef() throws GenericKnimeSparkException {
        return paramsAsJason(m_inputTableName, m_featureColIdxs, m_classColIdx, m_numIterations, m_regularization,
            m_useSGD, m_numCorrections, m_tolerance, m_UpdaterType, m_ValidateData, m_AddIntercept,
            m_UseFeatureScaling, m_GradientType, m_StepSize, m_Fraction);
    }


    /**
     * only values that are explicitly marked as Nullable are truly optional, the others are only checked for null so
     * that we can unit test the job validation
     * @param aInputRDD
     * @param featureColIdxs
     * @param classColIdx
     * @param aNumIterations
     * @param aRegularization
     * @param aUseSGD
     *
     * @param aTolerance - only required when aUseSGD == false
     * @param aNumCorrections - only required when aUseSGD == false
     * @param aUpdaterType
     * @param aValidateData
     * @param aAddIntercept
     * @param aUseFeatureScaling
     * @param aGradientType
     * @param aStepSize  - only required when aUseSGD == true
     * @param aFraction  - only required when aUseSGD == true
     * @return JSON representation of parameters
     *
     * @throws GenericKnimeSparkException
     */
    public static String paramsAsJason(final String aInputRDD, final Integer[] featureColIdxs,
        final Integer classColIdx, final Integer aNumIterations, final Double aRegularization, final Boolean aUseSGD,
        @Nullable final Integer aNumCorrections, @Nullable final Double aTolerance, final UpdaterType aUpdaterType,
        final Boolean aValidateData, final Boolean aAddIntercept, final Boolean aUseFeatureScaling,
        final GradientType aGradientType, @Nullable final Double aStepSize, @Nullable final Double aFraction)
        throws GenericKnimeSparkException {
        final List<Object> inputParams = new ArrayList<>();
        inputParams.add(KnimeSparkJob.PARAM_INPUT_TABLE);
        inputParams.add(aInputRDD);

        if (classColIdx != null) {
            inputParams.add(ParameterConstants.PARAM_LABEL_INDEX);
            inputParams.add(classColIdx);
        }

        if (featureColIdxs != null && featureColIdxs.length > 0) {
            inputParams.add(ParameterConstants.PARAM_COL_IDXS);
            inputParams.add(JsonUtils.toJsonArray((Object[])featureColIdxs));
        }

        if (aUpdaterType != null) {
            inputParams.add(AbstractRegularizationJob.PARAM_UPDATER_TYPE);
            inputParams.add(aUpdaterType.toString());
        }

        if (aValidateData != null) {
            inputParams.add(AbstractRegularizationJob.PARAM_VALIDATE_DATA);
            inputParams.add(aValidateData);
        }

        if (aAddIntercept != null) {
            inputParams.add(AbstractRegularizationJob.PARAM_ADD_INTERCEPT);
            inputParams.add(aAddIntercept);
        }
        if (aUseFeatureScaling != null) {
            inputParams.add(AbstractRegularizationJob.PARAM_USE_FEATURE_SCALING);
            inputParams.add(aUseFeatureScaling);
        }
        if (aGradientType != null) {
            inputParams.add(AbstractRegularizationJob.PARAM_GRADIENT_TYPE);
            inputParams.add(aGradientType);
        }

        if (aUseSGD != null) {
            inputParams.add(LogisticRegressionJob.PARAM_USE_SGD);
            inputParams.add(aUseSGD);
            if (aUseSGD) {
                if (aStepSize != null) {
                    inputParams.add(AbstractRegularizationJob.PARAM_STEP_SIZE);
                    inputParams.add(aStepSize);
                }
                if (aFraction != null) {
                    inputParams.add(AbstractRegularizationJob.PARAM_FRACTION);
                    inputParams.add(aFraction);
                }
            } else {
                if (aTolerance != null) {
                    inputParams.add(LogisticRegressionJob.PARAM_TOLERANCE);
                    inputParams.add(aTolerance);
                }
                if (aNumCorrections != null) {
                    inputParams.add(LogisticRegressionJob.PARAM_NUM_CORRECTIONS);
                    inputParams.add(aNumCorrections);
                }
            }
        }

        if (aNumIterations != null) {
            inputParams.add(AbstractRegularizationJob.PARAM_NUM_ITERATIONS);
            inputParams.add(aNumIterations);
        }

        if (aRegularization != null) {
            inputParams.add(AbstractRegularizationJob.PARAM_REGULARIZATION);
            inputParams.add(aRegularization);
        }

        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            inputParams.toArray(new Object[inputParams.size()]), ParameterConstants.PARAM_OUTPUT, new String[]{}});
    }
}
