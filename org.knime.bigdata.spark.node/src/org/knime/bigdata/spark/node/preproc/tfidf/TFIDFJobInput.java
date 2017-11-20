/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
package org.knime.bigdata.spark.node.preproc.tfidf;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * converts a single String column to a (sparse) Vector of terms and appends the Vector to the input RDD<Row>
 *
 * note that if we want to use this then we need to check that all the other jobs (and utils) can handle Row instances with Vector cells !!!
 *
 * @author dwk
 */
@SparkClass
public class TFIDFJobInput extends JobInput {

    private final String STRING_COL_ID = "stringColId";
    private final String MAX_NUM_TERMS = "maxNumTerms";
    private final String MIN_FREQUENCY = "minFrequency";
    private final String TERM_SEPARATOR = "termSeparator";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public TFIDFJobInput() {}

    TFIDFJobInput(final String namedInputObject, final String namedOutputObject, final Integer colIdx, final Integer maxNumTerms, final Integer minFrequency,
        final String termSeparator) {
        addNamedInputObject(namedInputObject);
        addNamedOutputObject(namedOutputObject);
        set(STRING_COL_ID, colIdx);
        set(MAX_NUM_TERMS, maxNumTerms);
        set(MIN_FREQUENCY, minFrequency);
        set(TERM_SEPARATOR,termSeparator);
    }

    /**
     * @return the column index containing the strings
     */
    public Integer getColIdx() {
        return getInteger(STRING_COL_ID);
    }

    /**
     * @return the maximum number of terms
     */
    public Integer getMaxNumTerms() {
        return getInteger(MAX_NUM_TERMS);
    }

    /**
     * @return the minimum frequency
     */
    public Integer getMinFrequency() {
        return getInteger(MIN_FREQUENCY);
    }

    /**
     * @return the term separator
     */
    public String getTermSeparator() {
        return get(TERM_SEPARATOR);
    }
//    //note aMaxNumTerms - smaller values lead to more hashing collisions
//    TFIDFJobInput(final SparkRDD inputRDD, final Integer aColIdx, final Integer aMaxNumTerms, final Integer aMinFrequency,
//        final String aSeparator, final SparkRDD outputRDD) {
//        this(inputRDD.getContext(), inputRDD.getID(), aColIdx, aMaxNumTerms, aMinFrequency, aSeparator, outputRDD
//            .getID());
//    }
//
//    //unit testing constructor only
//    TFIDFJobInput(final SparkContextConfig aContext, final String aInputRDD, final Integer aColIdx,
//        final Integer aMaxNumTerms, final Integer aMinFrequency, final String aSeparator, final String aOutputTable) {
//        m_stringColId = aColIdx;
//        m_maxNumTerms = aMaxNumTerms;
//        m_termSeparator = aSeparator;
//        m_context = aContext;
//        m_inputTableName = aInputRDD;
//        m_minFrequency = aMinFrequency;
//        m_outputTableName = aOutputTable;
//    }
//
//    void execute(final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
//        final String learnerParams = learnerDef();
//        if (exec != null) {
//            exec.checkCanceled();
//        }
//        JobserverJobController.startJobAndWaitForResult(m_context, TFIDFJob.class.getCanonicalName(), learnerParams, exec);
//    }
//
//    /**
//     * names of the columns (must include label column), required for value mapping info
//     *
//     * @throws KNIMESparkException
//     */
//    String learnerDef() throws KNIMESparkException {
//        return paramsAsJason(m_inputTableName, m_stringColId, m_maxNumTerms, m_minFrequency, m_termSeparator,
//            m_outputTableName);
//    }
//
//    static String paramsAsJason(final String aInputRDD, final Integer aColIdx, final Integer aMaxNumTerms,
//        final Integer aMinFrequency, final String aSeparator, final String aOutputTable)
//        throws KNIMESparkException {
//        final List<Object> inputParams = new ArrayList<>();
//        if (aInputRDD != null) {
//            inputParams.add(KnimeSparkJob.PARAM_INPUT_TABLE);
//            inputParams.add(aInputRDD);
//        }
//
//        if (aColIdx != null) {
//            inputParams.add(TFIDFJob.PARAM_COL_INDEX);
//            inputParams.add(aColIdx);
//        }
//
//        if (aMaxNumTerms != null) {
//            inputParams.add(TFIDFJob.PARAM_MAX_NUM_TERMS);
//            inputParams.add(aMaxNumTerms);
//        }
//        if (aMinFrequency != null) {
//            inputParams.add(TFIDFJob.PARAM_MIN_FREQUENCY);
//            inputParams.add(aMinFrequency);
//        }
//        if (aSeparator != null) {
//            inputParams.add(TFIDFJob.PARAM_SEPARATOR);
//            inputParams.add(aSeparator);
//        }
//
//        final String[] outputParams;
//        if (aOutputTable != null) {
//            outputParams = new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, aOutputTable};
//        } else {
//            outputParams = new String[]{};
//        }
//        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
//            inputParams.toArray(new Object[inputParams.size()]), ParameterConstants.PARAM_OUTPUT, outputParams});
//    }

}
