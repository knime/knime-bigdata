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
package com.knime.bigdata.spark.node.preproc.convert.number2category;

import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.ConvertNominalValuesJob;
import com.knime.bigdata.spark.jobserver.jobs.MapValuesJob;
import com.knime.bigdata.spark.jobserver.server.ColumnBasedValueMapping;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkDataTable;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Number2CategoryConverterTask {

    private final String m_inputTableName;

    private final String m_outputTableName;

    private final KNIMESparkContext m_context;

    private ColumnBasedValueMapping m_map;

    private final boolean m_keepOriginalColumns;

    /**
     * @param inputRDD input RDD
     * @param map the {@link ColumnBasedValueMapping}
     * @param aKeepOriginalColumns keep original columns or not, default is true
     * @param aOutputRDD the name of the output RDD
     */
    public Number2CategoryConverterTask(final SparkDataTable inputRDD, final ColumnBasedValueMapping map,
        final boolean aKeepOriginalColumns, final String aOutputRDD) {
        this(inputRDD.getContext(), inputRDD.getID(), map, aKeepOriginalColumns, aOutputRDD);
    }

    Number2CategoryConverterTask(final KNIMESparkContext aContext, final String inputRDD,
        final ColumnBasedValueMapping map, final boolean aKeepOriginalColumns, final String aOutputRDD) {
        m_context = aContext;
        m_inputTableName = inputRDD;
        m_map = map;
        m_keepOriginalColumns = aKeepOriginalColumns;
        m_outputTableName = aOutputRDD;
    }

    /**
     * run the job on the server
     *
     * @param exec
     * @throws Exception
     */
    public void execute(final ExecutionMonitor exec) throws Exception {
        final String params = paramDef();
        if (exec != null) {
            exec.checkCanceled();
        }
        JobControler.startJobAndWaitForResult(m_context, MapValuesJob.class.getCanonicalName(), params, exec);
        return;
    }

    String paramDef() throws GenericKnimeSparkException {
        return paramDef(m_inputTableName, m_map, m_keepOriginalColumns, m_outputTableName);
    }

    /**
     * (for better unit testing)
     *
     * @param inputTableName
     * @param map the {@link ColumnBasedValueMapping}
     * @param outputTableName
     * @return Json String with parameter settings
     * @throws GenericKnimeSparkException
     */
    static String paramDef(final String inputTableName, final ColumnBasedValueMapping map,
        final boolean aKeepOriginalColumns, final String outputTableName) throws GenericKnimeSparkException {
        if (map == null) {
            throw new NullPointerException("Column Value Mapping must not be null!");
        }
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new Object[]{MapValuesJob.PARAM_MAPPING, JobConfig.encodeToBase64(map), KnimeSparkJob.PARAM_INPUT_TABLE,
                inputTableName, ConvertNominalValuesJob.PARAM_KEEP_ORIGINAL_COLUMNS, aKeepOriginalColumns},
            ParameterConstants.PARAM_OUTPUT, new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, outputTableName}});
    }

}
