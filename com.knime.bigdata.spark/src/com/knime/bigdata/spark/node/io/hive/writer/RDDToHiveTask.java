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

import java.util.ArrayList;

import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;
import org.knime.core.node.ExecutionContext;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.RDDToHiveJob;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.data.SparkRDD;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;


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
     * @param hiveQuery - the hive query to execute
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
    public void execute(final ExecutionContext exec) throws Exception {
        final String jsonArgs = params2Json();
        String jobId = JobControler.startJob(m_rdd.getContext(), RDDToHiveJob.class.getCanonicalName(), jsonArgs);
        JobResult result = JobControler.waitForJobAndFetchResult(jobId, exec);
        assert(m_rdd.getID().equals(result.getFirstTableKey()));
    }

    private final String params2Json() {

        ArrayList<ArrayList<String>> fields = new ArrayList<>();
        for (StructField field : m_schema.getFields()) {
            ArrayList<String> f = new ArrayList<>();
            f.add(field.getName());
            f.add(JobResult.getJavaTypeFromDataType(field.getDataType()).getCanonicalName());
            f.add("" + field.isNullable());
            fields.add(f);
        }
        final Config config = ConfigFactory.empty().withValue(ParameterConstants.PARAM_SCHEMA,
            ConfigValueFactory.fromIterable(fields));
        final String schema = config.root().render(ConfigRenderOptions.concise());
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_TABLE_1, m_rdd.getID(),
            ParameterConstants.PARAM_SCHEMA, schema},
            ParameterConstants.PARAM_OUTPUT, new String[]{ParameterConstants.PARAM_TABLE_1, m_tableName}});
    }

}
