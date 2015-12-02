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
package com.knime.bigdata.spark.node.preproc.tfidf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.TFIDFJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 * converts a single String column to a (sparse) Vector of terms and appends the Vector to the input RDD<Row>
 *
 * note that if we want to use this then we need to check that all the other jobs (and utils) can handle Row instances with Vector cells !!!
 *
 * @author dwk
 */
public class TFIDFTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int m_stringColId;

    private final KNIMESparkContext m_context;

    private final String m_inputTableName;

    private final Integer m_maxNumTerms;

    private final String m_termSeparator;

    private final Integer m_minFrequency;

    private final String m_outputTableName;

    //note aMaxNumTerms - smaller values lead to more hashing collisions
    TFIDFTask(final SparkRDD inputRDD, final Integer aColIdx, final Integer aMaxNumTerms, final Integer aMinFrequency,
        final String aSeparator, final SparkRDD outputRDD) {
        this(inputRDD.getContext(), inputRDD.getID(), aColIdx, aMaxNumTerms, aMinFrequency, aSeparator, outputRDD
            .getID());
    }

    //unit testing constructor only
    TFIDFTask(final KNIMESparkContext aContext, final String aInputRDD, final Integer aColIdx,
        final Integer aMaxNumTerms, final Integer aMinFrequency, final String aSeparator, final String aOutputTable) {
        m_stringColId = aColIdx;
        m_maxNumTerms = aMaxNumTerms;
        m_termSeparator = aSeparator;
        m_context = aContext;
        m_inputTableName = aInputRDD;
        m_minFrequency = aMinFrequency;
        m_outputTableName = aOutputTable;
    }

    void execute(final ExecutionMonitor exec) throws GenericKnimeSparkException, CanceledExecutionException {
        final String learnerParams = learnerDef();
        if (exec != null) {
            exec.checkCanceled();
        }
        JobControler.startJobAndWaitForResult(m_context, TFIDFJob.class.getCanonicalName(), learnerParams, exec);
    }

    /**
     * names of the columns (must include label column), required for value mapping info
     *
     * @throws GenericKnimeSparkException
     */
    String learnerDef() throws GenericKnimeSparkException {
        return paramsAsJason(m_inputTableName, m_stringColId, m_maxNumTerms, m_minFrequency, m_termSeparator,
            m_outputTableName);
    }

    static String paramsAsJason(final String aInputRDD, final Integer aColIdx, final Integer aMaxNumTerms,
        final Integer aMinFrequency, final String aSeparator, final String aOutputTable)
        throws GenericKnimeSparkException {
        final List<Object> inputParams = new ArrayList<>();
        if (aInputRDD != null) {
            inputParams.add(KnimeSparkJob.PARAM_INPUT_TABLE);
            inputParams.add(aInputRDD);
        }

        if (aColIdx != null) {
            inputParams.add(TFIDFJob.PARAM_COL_INDEX);
            inputParams.add(aColIdx);
        }

        if (aMaxNumTerms != null) {
            inputParams.add(TFIDFJob.PARAM_MAX_NUM_TERMS);
            inputParams.add(aMaxNumTerms);
        }
        if (aMinFrequency != null) {
            inputParams.add(TFIDFJob.PARAM_MIN_FREQUENCY);
            inputParams.add(aMinFrequency);
        }
        if (aSeparator != null) {
            inputParams.add(TFIDFJob.PARAM_SEPARATOR);
            inputParams.add(aSeparator);
        }

        final String[] outputParams;
        if (aOutputTable != null) {
            outputParams = new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, aOutputTable};
        } else {
            outputParams = new String[]{};
        }
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            inputParams.toArray(new Object[inputParams.size()]), ParameterConstants.PARAM_OUTPUT, outputParams});
    }

}
