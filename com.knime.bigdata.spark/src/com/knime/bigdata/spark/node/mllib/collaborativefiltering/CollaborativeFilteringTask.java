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
package com.knime.bigdata.spark.node.mllib.collaborativefiltering;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.CollaborativeFilteringJob;
import com.knime.bigdata.spark.jobserver.server.CollaborativeFilteringModel;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class CollaborativeFilteringTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Integer m_userIdx;

    private final KNIMESparkContext m_context;

    private final String m_inputTableName;

    private final Double m_lambda;

    private final int m_ratingIdx;

    private final int m_productIdx;

    /**
     * null indicates that default value will be used
     */
    private Double m_alpha = null;

    private Integer m_rank = null;

    private Integer m_numIterations = null;

    private Boolean m_IsNonNegative = null;

    private Integer m_NumBlocks = null;

    private Integer m_NumUserBlocks = null;

    private Integer m_NumProductBlocks = null;

    private Boolean m_IsImplicitPrefs = null;

    private Long m_randomSeed = null;

    CollaborativeFilteringTask(final SparkRDD inputRDD, final int aUserIdx, final int aProductIdx,
        final int aRatingIdx, final double aLambda, final double aAlpha, final int aIterations, final int aRank,
        final boolean implicitPrefs, final int noOfBlocks) {
        this(inputRDD.getContext(), inputRDD.getID(), aUserIdx, aProductIdx, aRatingIdx, aLambda, aAlpha, aIterations,
            aRank, implicitPrefs, noOfBlocks);
    }

    CollaborativeFilteringTask(final KNIMESparkContext aContext, final String aInputRDD, final int aUserIdx,
        final int aProductIdx, final int aRatingIdx, final Double aLambda, final Double aAlpha,
        final Integer aIterations, final Integer aRank, final boolean implicitPrefs, final int noOfBlocks) {
        m_lambda = aLambda;
        m_context = aContext;
        m_inputTableName = aInputRDD;

        //note that user and product columns must be integer valued
        m_userIdx = aUserIdx;
        m_productIdx = aProductIdx;

        //rating column should be double valued
        m_ratingIdx = aRatingIdx;
        m_alpha = aAlpha;
        m_numIterations = aIterations;
        m_rank = aRank;
        m_NumBlocks = noOfBlocks;
        m_IsImplicitPrefs = implicitPrefs;
    }

    CollaborativeFilteringModel execute(final ExecutionContext exec, final String aResultTableName) throws GenericKnimeSparkException,
        CanceledExecutionException {
        final String learnerParams = paramsAsJason(aResultTableName);
        if (exec != null) {
            exec.checkCanceled();
        }
        final JobResult result = JobControler.startJobAndWaitForResult(m_context,
            CollaborativeFilteringJob.class.getCanonicalName(), learnerParams, exec);

        return (CollaborativeFilteringModel)result.getObjectResult();
    }

    String paramsAsJason(final String aResultTableName) {
        return paramsAsJason(m_inputTableName, m_userIdx, m_productIdx, m_ratingIdx, m_lambda, m_alpha, m_rank,
            m_numIterations, m_IsNonNegative, m_NumBlocks, m_NumUserBlocks,
            m_NumProductBlocks, m_IsImplicitPrefs, m_randomSeed, aResultTableName);
    }

    /**
     * (non-private for unit testing)
     *
     * @param aInputTableName
     * @param aLabelColIndex
     * @param aNumericColIdx
     * @param aLambda
     * @param aIsNonNegative
     * @param aNumBlocks
     * @param aNumUserBlocks
     * @param aNumProductBlocks
     * @param aIsImplicitPrefs
     * @param aSeed
     * @return Json representation of parameters
     */
    static String paramsAsJason(final String aInputTableName, final Integer aUserIndex, final Integer aProductIndex,
        final Integer aRatingIndex, @Nullable final Double aLambda, @Nullable final Double aAlpha, final Integer aRank,
        final Integer aNumIterations, final Boolean aIsNonNegative, final Integer aNumBlocks, final Integer aNumUserBlocks,
        final Integer aNumProductBlocks, final Boolean aIsImplicitPrefs, final Long aSeed, final String aResultTableName) {

        final List<Object> inputParams = new ArrayList<>();
        inputParams.add(KnimeSparkJob.PARAM_INPUT_TABLE);
        inputParams.add(aInputTableName);
        if (aUserIndex != null) {
            inputParams.add(CollaborativeFilteringJob.PARAM_USER_INDEX);
            inputParams.add(aUserIndex);
        }
        if (aProductIndex != null) {
            inputParams.add(CollaborativeFilteringJob.PARAM_PRODUCT_INDEX);
            inputParams.add(aProductIndex);
        }
        if (aRatingIndex != null) {
            inputParams.add(CollaborativeFilteringJob.PARAM_RATING_INDEX);
            inputParams.add(aRatingIndex);
        }
        if (aLambda != null) {
            inputParams.add(CollaborativeFilteringJob.PARAM_LAMBDA);
            inputParams.add(aLambda);
        }
        if (aAlpha != null) {
            inputParams.add(CollaborativeFilteringJob.PARAM_ALPHA);
            inputParams.add(aAlpha);
        }
        if (aRank != null) {
            inputParams.add(CollaborativeFilteringJob.PARAM_RANK);
            inputParams.add(aRank);
        }
        if (aNumIterations != null) {
            inputParams.add(CollaborativeFilteringJob.PARAM_NUM_ITERATIONS);
            inputParams.add(aNumIterations);
        }
        //further parameters
        if (aIsNonNegative != null) {
            inputParams.add(CollaborativeFilteringJob.PARAM_IS_NON_NEGATIVE);
            inputParams.add(aIsNonNegative);
        }
        if (aNumBlocks != null) {
            inputParams.add(CollaborativeFilteringJob.PARAM_NUM_BLOCKS);
            inputParams.add(aNumBlocks);
        }
        //
        //
        if (aNumUserBlocks != null) {
            inputParams.add(CollaborativeFilteringJob.PARAM_NUM_USER_BLOCKS);
            inputParams.add(aNumUserBlocks);
        }
        //
        if (aNumProductBlocks != null) {
            inputParams.add(CollaborativeFilteringJob.PARAM_NUM_PRODUCT_BLOCKS);
            inputParams.add(aNumProductBlocks);
        }
        //
        if (aIsImplicitPrefs != null) {
            inputParams.add(CollaborativeFilteringJob.PARAM_IMPLICIT_PREFS);
            inputParams.add(aIsImplicitPrefs);
        }
        //
        if (aSeed != null) {
            inputParams.add(ParameterConstants.PARAM_SEED);
            inputParams.add(aSeed);
        }
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            inputParams.toArray(new Object[inputParams.size()]), ParameterConstants.PARAM_OUTPUT,
            new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, aResultTableName}});
    }

    /**
     * set alpha parameter
     *
     * @param aAlpha
     * @return this
     */
    public CollaborativeFilteringTask withAlpha(final double aAlpha) {
        m_alpha = aAlpha;
        return this;
    }

    /**
     * @param aRank
     * @return this
     */
    public CollaborativeFilteringTask withRank(final int aRank) {
        m_rank = aRank;
        return this;
    }

    /**
     * @param aNumIterations
     * @return this
     */
    public CollaborativeFilteringTask withNumIterations(final int aNumIterations) {
        m_numIterations = aNumIterations;
        return this;
    }
}
