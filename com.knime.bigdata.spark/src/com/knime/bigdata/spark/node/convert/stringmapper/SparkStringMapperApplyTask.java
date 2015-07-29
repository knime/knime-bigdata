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
package com.knime.bigdata.spark.node.convert.stringmapper;

import org.knime.core.node.ExecutionContext;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.ConvertNominalValuesJob;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.MappedRDDContainer;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkStringMapperApplyTask {

    private final String m_inputTableName;

    private final String m_outputTableName;

    private final String m_mappingTableName;

    private final Integer[] m_includeColIdxs;

    private final String[] m_includeColNames;

    private final KNIMESparkContext m_context;

    /**
     * constructor - simply stores parameters
     *
     * @param inputRDD input RDD
     * @param aMappingRDD name of RDD with mapping info
     * @param includeColIdxs - indices of the columns to include starting with 0
     * @param aIncludedColsNames
     * @param aOutputRDD - table identifier (output data)
     */
    public SparkStringMapperApplyTask(final SparkRDD inputRDD, final String aMappingRDD, final int[] includeColIdxs,
        final String[] aIncludedColsNames, final String aOutputRDD) {

        m_context = inputRDD.getContext();
        m_inputTableName = inputRDD.getID();
        m_includeColIdxs = new Integer[includeColIdxs.length];
        int i = 0;
        for (int value : includeColIdxs) {
            m_includeColIdxs[i++] = Integer.valueOf(value);
        }
        m_includeColNames = aIncludedColsNames;
        m_outputTableName = aOutputRDD;
        m_mappingTableName = aMappingRDD;
    }

    /**
     * run the job on the server
     *
     * @param exec
     * @return NominalValueMapping the mapping
     * @throws Exception
     */
    public MappedRDDContainer execute(final ExecutionContext exec) throws Exception {
        final String params = paramDef();
        final String jobId = JobControler.startJob(m_context, ConvertNominalValuesJob.class.getCanonicalName(), params);

        final JobResult result = JobControler.waitForJobAndFetchResult(m_context, jobId, exec);
        return (MappedRDDContainer)result.getObjectResult();
    }

    private String paramDef() {
        return paramDef(m_inputTableName, m_mappingTableName, m_includeColIdxs, m_includeColNames,
            m_outputTableName);
    }

    /**
     * (for better unit testing)
     *
     * @param includeColIdxs
     * @param includeColNames
     * @param inputTableName
     * @param aMappingTableName
     * @param outputTableName
     * @return Json String with parameter settings
     */
    public static String paramDef(final String inputTableName, final String aMappingTableName,
        final Integer[] includeColIdxs, final String[] includeColNames, final String outputTableName) {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new Object[]{ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray((Object[])includeColIdxs),
                ParameterConstants.PARAM_COL_IDXS + ParameterConstants.PARAM_STRING,
                JsonUtils.toJsonArray((Object[])includeColNames), ParameterConstants.PARAM_TABLE_1, inputTableName,
                ParameterConstants.PARAM_TABLE_2, aMappingTableName}, ParameterConstants.PARAM_OUTPUT,
            new String[]{ParameterConstants.PARAM_TABLE_1, outputTableName}});
    }

}
