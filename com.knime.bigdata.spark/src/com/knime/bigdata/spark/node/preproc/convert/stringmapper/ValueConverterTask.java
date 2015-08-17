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
package com.knime.bigdata.spark.node.preproc.convert.stringmapper;

import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.ConvertNominalValuesJob;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.MappedRDDContainer;
import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class ValueConverterTask {

    private final String m_inputTableName;

    private final String m_outputTableName;

    private final String m_outputMappingTableName;

    private final MappingType m_mappingType;

    private final Integer[] m_includeColIdxs;

    private final String[] m_includeColNames;

    private final KNIMESparkContext m_context;

    /**
     * constructor - simply stores parameters
     *
     * @param inputRDD input RDD
     * @param includeColIdxs - indices of the columns to include starting with 0
     * @param aIncludedColsNames
     * @param aMappingType - type of value mapping (global, per column or binary)
     * @param aOutputRDD - table identifier (output data)
     * @param aOutputMappingRDD
     */
    public ValueConverterTask(final SparkRDD inputRDD, final int[] includeColIdxs, final String[] aIncludedColsNames,
        final MappingType aMappingType, final String aOutputRDD, final String aOutputMappingRDD) {

        m_context = inputRDD.getContext();
        m_inputTableName = inputRDD.getID();
        m_includeColIdxs = new Integer[includeColIdxs.length];
        int i = 0;
        for (int value : includeColIdxs) {
            m_includeColIdxs[i++] = Integer.valueOf(value);
        }
        m_includeColNames = aIncludedColsNames;
        m_outputTableName = aOutputRDD;
        m_outputMappingTableName = aOutputMappingRDD;
        m_mappingType = aMappingType;
    }

    /**
     * run the job on the server
     *
     * @param exec
     * @return NominalValueMapping the mapping
     * @throws Exception
     */
    public MappedRDDContainer execute(final ExecutionMonitor exec) throws Exception {
        final String params = paramDef();
        exec.checkCanceled();
        final String jobId = JobControler.startJob(m_context, ConvertNominalValuesJob.class.getCanonicalName(), params);

        final JobResult result = JobControler.waitForJobAndFetchResult(m_context, jobId, exec);
        return (MappedRDDContainer)result.getObjectResult();
    }

    private String paramDef() {
        return paramDef(m_includeColIdxs, m_includeColNames, m_mappingType.toString(), m_inputTableName, m_outputTableName,
            m_outputMappingTableName);
    }

    /**
     * (for better unit testing)
     * @param includeColIdxs
     * @param includeColNames
     * @param mappingType
     * @param inputTableName
     * @param outputTableName
     * @param outputMappingTableName
     * @return Json String with parameter settings
     */
    public static String paramDef(final Integer[] includeColIdxs, final String[] includeColNames, final String mappingType,
        final String inputTableName, final String outputTableName, final String outputMappingTableName) {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new Object[]{ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray((Object[])includeColIdxs),
                ParameterConstants.PARAM_COL_IDXS + ParameterConstants.PARAM_STRING,
                JsonUtils.toJsonArray((Object[])includeColNames), ParameterConstants.PARAM_STRING,
                mappingType, ParameterConstants.PARAM_TABLE_1, inputTableName},
            ParameterConstants.PARAM_OUTPUT,
            new String[]{ParameterConstants.PARAM_TABLE_1, outputTableName, ParameterConstants.PARAM_TABLE_2,
                outputMappingTableName}});
    }

}
