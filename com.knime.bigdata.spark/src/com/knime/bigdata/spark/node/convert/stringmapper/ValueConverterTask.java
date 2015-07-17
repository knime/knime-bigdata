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

import org.apache.spark.sql.api.java.StructType;
import org.knime.core.node.ExecutionContext;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.ConvertNominalValuesJob;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.jobserver.server.NominalValueMapping;
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

    private final KNIMESparkContext m_context;

    /**
     * constructor - simply stores parameters
     * @param inputRDD input RDD
     * @param includeColIdxs - indices of the columns to include starting with 0
     * @param aMappingType - type of value mapping (global, per column or binary)
     * @param aOutputRDD - table identifier (output data)
     * @param aOutputMappingRDD
     */
    public ValueConverterTask(final SparkRDD inputRDD, final int[] includeColIdxs, final MappingType aMappingType,
        final String aOutputRDD, final String aOutputMappingRDD) {

        m_context = inputRDD.getContext();
        m_inputTableName = inputRDD.getID();
        m_includeColIdxs = new Integer[includeColIdxs.length];
        int i = 0;
        for (int value : includeColIdxs) {
            m_includeColIdxs[i++] = Integer.valueOf(value);
        }
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
    public NominalValueMapping execute(final ExecutionContext exec) throws Exception {
        final String params = paramDef();
        final String jobId =
                JobControler.startJob(m_context, ConvertNominalValuesJob.class.getCanonicalName(), params);

        final JobResult result = JobControler.waitForJobAndFetchResult(m_context, jobId, exec);
        //TODO - not sure whether this is of any help
        StructType tableSchema = result.getTables().get(m_outputTableName);

        //TODO - need to create result data table specs

        NominalValueMapping mapping = (NominalValueMapping)result.getObjectResult();

        return mapping;

    }

    private String paramDef() {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new Object[]{ParameterConstants.PARAM_COL_IDXS, JsonUtils.toJsonArray(m_includeColIdxs),
                ParameterConstants.PARAM_STRING, m_mappingType.toString(), ParameterConstants.PARAM_TABLE_1, m_inputTableName},
            ParameterConstants.PARAM_OUTPUT, new String[]{ParameterConstants.PARAM_TABLE_1, m_outputTableName,
                ParameterConstants.PARAM_TABLE_2, m_outputMappingTableName}});
    }

}

