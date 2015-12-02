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
package com.knime.bigdata.spark.node.io.hive.writer;

import org.apache.spark.sql.api.java.StructType;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.RDDToHiveJob;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkRDD;


/**
 *
 * @author dwk, jfr
 */
public class RDDToHiveTask {

    private final SparkRDD m_rdd;

    private final String m_tableName;

    private StructType m_schema;

    /**
     * constructor - simply stores parameters
     * @param rdd - the {@link SparkRDD} to generate
     * @param tableName the name of the table
     * @param schema the schema of the RDD
     */
    public RDDToHiveTask(final SparkRDD rdd, final String tableName, final StructType schema) {
        m_rdd = rdd;
        m_tableName = tableName;
        m_schema = schema;
    }

    /**
     * run the job on the server
     * @param exec execution context
     * @throws Exception if anything goes wrong
     */
    public void execute(final ExecutionMonitor exec) throws Exception {
        final String jsonArgs = params2Json();
        final KNIMESparkContext context = m_rdd.getContext();
        exec.checkCanceled();
        JobControler.startJobAndWaitForResult(context, RDDToHiveJob.class.getCanonicalName(), jsonArgs, exec);
    }

    private final String params2Json() {
        final String schema = StructTypeBuilder.toConfigString(m_schema);
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            new String[]{KnimeSparkJob.PARAM_INPUT_TABLE, m_rdd.getID(),
            ParameterConstants.PARAM_SCHEMA, schema},
            ParameterConstants.PARAM_OUTPUT, new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, m_tableName}});
    }

}
