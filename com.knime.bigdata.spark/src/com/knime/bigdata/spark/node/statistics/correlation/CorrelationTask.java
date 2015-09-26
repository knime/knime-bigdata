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
package com.knime.bigdata.spark.node.statistics.correlation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.knime.base.util.HalfDoubleMatrix;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.CorrelationJob;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.CorrelationMethods;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.HalfDoubleMatrixFromLinAlgMatrix;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 *
 * @author dwk
 */
public class CorrelationTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final KNIMESparkContext m_context;

    private final Integer[] m_colIds;

    private final CorrelationMethods m_statMethod;

    private final String m_inputTableName;

    private final String m_outputTableName;

    private final boolean m_returnCorrelationMatrix;

    /**
     *
     * @param inputRDD
     * @param aColIdxs
     * @param aStatMethod
     * @param aOutputTable - null value indicates that the correlation of exactly 2 indices is to be computed and
     *            returned, otherwise a matrix with all correlations between all given indices is constructed and stored
     *            in an RDD
     */
    public CorrelationTask(final SparkRDD inputRDD, final Integer[] aColIdxs, final CorrelationMethods aStatMethod,
        @Nullable final String aOutputTable, final boolean aReturnCorrelationMatrix) {
        this(inputRDD.getContext(), inputRDD.getID(), aColIdxs, aStatMethod, aOutputTable, aReturnCorrelationMatrix);
    }

    public CorrelationTask(final KNIMESparkContext aContext, final String aInputRDD, final Integer[] aColIds,
        final CorrelationMethods aStatMethod, @Nullable final String aOutputTable,
        final boolean aReturnCorrelationMatrix) {
        m_context = aContext;
        m_inputTableName = aInputRDD;
        m_colIds = aColIds;
        m_statMethod = aStatMethod;
        m_outputTableName = aOutputTable;
        m_returnCorrelationMatrix = aReturnCorrelationMatrix;
    }

    public HalfDoubleMatrix execute(final ExecutionMonitor exec) throws GenericKnimeSparkException, CanceledExecutionException {
        final String jasonParams = paramsAsJason();
        if (exec != null) {
            exec.checkCanceled();
        }
        JobResult res =
            JobControler
                .startJobAndWaitForResult(m_context, CorrelationJob.class.getCanonicalName(), jasonParams, exec);
        if (m_returnCorrelationMatrix) {
            HalfDoubleMatrixFromLinAlgMatrix serverMatrix = (HalfDoubleMatrixFromLinAlgMatrix)res.getObjectResult();
            final int nRows = serverMatrix.getRowCount();
            final boolean withDiagonal = serverMatrix.storesDiagonal();
            HalfDoubleMatrix mat = new HalfDoubleMatrix(nRows, withDiagonal);
            for (int r = 0; r < nRows; r++) {
                for (int c = r; c < nRows; c++) {
                    if (c != r || withDiagonal) {
                        mat.set(r, c, serverMatrix.get(r, c));
                    }
                }
            }
            return mat;
        } else if (m_outputTableName != null) {
            HalfDoubleMatrix mat = new HalfDoubleMatrix(1, true);
            mat.set(0, 0, Double.MIN_VALUE);
            return mat;
        } else {
            HalfDoubleMatrix mat = new HalfDoubleMatrix(1, true);
            mat.set(0, 0, (Double)res.getObjectResult());
            return mat;
        }
    }

    String paramsAsJason() {
        return paramsAsJason(m_inputTableName, m_colIds, m_statMethod, m_outputTableName, m_returnCorrelationMatrix);
    }

    /**
     * (non-private for unit testing)
     *
     * @return Json representation of parameters
     */
    static String paramsAsJason(final String aInputTableName, final Integer[] aColIds,
        final CorrelationMethods aMethod, @Nullable final String aResTable, final Boolean aReturnMatrix) {

        if (aColIds == null || aColIds.length < 2) {
            throw new NullPointerException("Need at least two column indices for correlation computation.");
        }
        if (aColIds.length > 2 && aResTable == null) {
            throw new NullPointerException(
                "Too many column indices given. Need either two column indices for correlation computation or output table");
        }

        final List<Object> inputParams = new ArrayList<>();
        inputParams.add(KnimeSparkJob.PARAM_INPUT_TABLE);
        inputParams.add(aInputTableName);
        inputParams.add(ParameterConstants.PARAM_COL_IDXS);
        inputParams.add(JsonUtils.toJsonArray((Object[])aColIds));

        final List<Object> outputParams = new ArrayList<>();

        if (aResTable != null) {
            outputParams.add(KnimeSparkJob.PARAM_RESULT_TABLE);
            outputParams.add(aResTable);
        }
        if (aReturnMatrix != null && aReturnMatrix) {
            outputParams.add(CorrelationJob.PARAM_RETURN_MATRIX);
            outputParams.add(aReturnMatrix);
        }

        inputParams.add(CorrelationJob.PARAM_STAT_METHOD);
        inputParams.add(aMethod);
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            inputParams.toArray(new Object[inputParams.size()]), ParameterConstants.PARAM_OUTPUT,
            outputParams.toArray(new Object[outputParams.size()])});
    }
}
