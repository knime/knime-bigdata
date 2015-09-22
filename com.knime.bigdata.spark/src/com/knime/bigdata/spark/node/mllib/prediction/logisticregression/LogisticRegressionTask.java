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
package com.knime.bigdata.spark.node.mllib.prediction.logisticregression;

import java.io.Serializable;

import javax.annotation.Nullable;

import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.jobs.LogisticRegressionJob;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.GradientType;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.UpdaterType;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.node.mllib.prediction.linear.SGDLearnerTask;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 *
 * @author dwk
 */
public class LogisticRegressionTask implements Serializable {

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

    /**
     * create a logistic regression task that uses SGD
     * @param inputRDD
     * @param featureColIdxs
     * @param classColIdx
     * @param aNumIterations
     * @param aRegularization
     * @param aUseSGD
     * @param aNumCorrections - only required when aUseSGD == false
     * @param aTolerance - only required when aUseSGD == false
     * @param aStepSize - only required when aUseSGD == true
     * @param aFraction - only required when aUseSGD == true
     */
    LogisticRegressionTask(final SparkRDD inputRDD, final Integer[] featureColIdxs, final int classColIdx,
        final int aNumIterations, final double aRegularization,
        final UpdaterType aUpdaterType,
        final Boolean aValidateData, final Boolean aAddIntercept, final Boolean aUseFeatureScaling,
        final GradientType aGradientType, final Double aStepSize,final Double aFraction) {
        this(inputRDD.getContext(), inputRDD.getID(), featureColIdxs, classColIdx, aNumIterations, aRegularization,
            true, null, null, aUpdaterType, aValidateData, aAddIntercept, aUseFeatureScaling, aGradientType, aStepSize, aFraction);
    }

    /**
     * create a logistic regression task that uses LBFGS
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
    LogisticRegressionTask(final SparkRDD inputRDD, final Integer[] featureColIdxs, final int classColIdx,
        final int aNumIterations, final double aRegularization,
        final Integer aNumCorrections, final Double aTolerance, final UpdaterType aUpdaterType,
        final Boolean aValidateData, final Boolean aAddIntercept, final Boolean aUseFeatureScaling,
        final GradientType aGradientType) {
        this(inputRDD.getContext(), inputRDD.getID(), featureColIdxs, classColIdx, aNumIterations, aRegularization,
            false, aNumCorrections, aTolerance, aUpdaterType, aValidateData, aAddIntercept, aUseFeatureScaling, aGradientType, null, null);
    }

    //unit testing constructor only
    LogisticRegressionTask(final KNIMESparkContext aContext, final String aInputRDD, final Integer[] featureColIdxs,
        final int classColIdx, final int aNumIterations, final double aRegularization, final boolean aUseSGD,
        @Nullable final Integer aNumCorrections, @Nullable final Double aTolerance, final UpdaterType aUpdaterType,
        final Boolean aValidateData, final Boolean aAddIntercept, final Boolean aUseFeatureScaling,
        final GradientType aGradientType, @Nullable final Double aStepSize, @Nullable final Double aFraction) {
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
    }

    LogisticRegressionModel execute(final ExecutionMonitor exec) throws GenericKnimeSparkException,
        CanceledExecutionException {
        final String learnerParams = learnerDef();
        if (exec != null) {
            exec.checkCanceled();
        }
        final JobResult result =
            JobControler.startJobAndWaitForResult(m_context, LogisticRegressionJob.class.getCanonicalName(),
                learnerParams, exec);

        return (LogisticRegressionModel)result.getObjectResult();
    }

    /**
     * names of the columns (must include label column), required for value mapping info
     *
     * @throws GenericKnimeSparkException
     */
    String learnerDef() throws GenericKnimeSparkException {
        return SGDLearnerTask.paramsAsJason(m_inputTableName, m_featureColIdxs, m_classColIdx, m_numIterations, m_regularization,
            m_useSGD, m_numCorrections, m_tolerance, m_UpdaterType, m_ValidateData, m_AddIntercept,
            m_UseFeatureScaling, m_GradientType, m_StepSize, m_Fraction);
    }


}
