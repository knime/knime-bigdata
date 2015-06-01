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
 *   Created on 29.05.2015 by koetter
 */
package com.knime.bigdata.spark.node.scripting.snippet;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nonnull;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.jar.SparkJobCompiler;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.node.scripting.AbstractSparkSnippetNodeModel;
import com.knime.bigdata.spark.port.data.AbstractSparkRDD;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDGenerator;

/**
 *
 * @author koetter
 */
public class SparkSnippetNodeModel extends AbstractSparkSnippetNodeModel {

    /** Constructor. */
    public SparkSnippetNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE, SparkDataPortObject.TYPE_OPTIONAL},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        SparkDataPortObject data1 = (SparkDataPortObject)inData[0];
        AbstractSparkRDD table1 = data1.getData();
        SparkDataPortObject data2 = (SparkDataPortObject)inData[1];
        String table2Name = null;
        if (data2 != null) {
            //we have two incoming RDDs
            SparkDataTable table2 = data2.getData();
            if (!table1.compatible(table2)) {
                throw new InvalidSettingsException("Input objects belong to two different Spark contexts");
            }
            table2Name = table2.getID();
        }
        String tableName = SparkIDGenerator.createID();
        String jobDescription = getCode();

        //first replace table placeholders by apply method argument names:
        jobDescription = jobDescription.replace("$table1", "aInput1");
        if (table2Name != null) {
            jobDescription = jobDescription.replace("$table2", "aInput2");
        }
        //now compile code, add to jar and upload jar:
        final String jobClassName = addTransformationJob2Jar(jobDescription);

        //call the Spark job with the two rdds and use the tableName as id for the result RDD and
        //the job description as source code for the job

        //start job with proper parameters
        String jobId = JobControler.startJob(table1.getContext(), jobClassName, params2Json(table1.getID(), table2Name, tableName));

        final JobResult result = JobControler.waitForJobAndFetchResult(jobId, exec);

        final List<DataColumnSpec> specs = new LinkedList<>();
        final DataColumnSpecCreator specCreator = new DataColumnSpecCreator("Test", StringCell.TYPE);
        specs.add(specCreator.createSpec());
        specCreator.setName("Test2");
        specCreator.setType(DoubleCell.TYPE);
        specs.add(specCreator.createSpec());
        final DataTableSpec resultSpec = new DataTableSpec(specs.toArray(new DataColumnSpec[0]));
        SparkDataTable resultTable = new SparkDataTable(data1.getContext(), tableName, resultSpec);
        final SparkDataPortObject resultObject = new SparkDataPortObject(resultTable);
        return new PortObject[]{resultObject};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new PortObjectSpec[]{null};
    }

    private final String params2Json(@Nonnull final String aInputTable1, final String aInputTable2, @Nonnull final String aOutputTable) {
        final String[] inputParams;
        if (aInputTable2 != null) {
            inputParams = new String[]{ParameterConstants.PARAM_TABLE_1, aInputTable1,
                ParameterConstants.PARAM_TABLE_2, aInputTable2};
        } else {
            inputParams = new String[]{ParameterConstants.PARAM_TABLE_1, aInputTable1};
        }
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            inputParams, ParameterConstants.PARAM_OUTPUT,
            new String[]{ParameterConstants.PARAM_TABLE_1, aOutputTable}});
    }

    private String addTransformationJob2Jar(final String aTransformationCode)
        throws GenericKnimeSparkException {

        final String jarPath;
        try {
            File f = File.createTempFile("knimeJobUtils", ".jar");
            f.deleteOnExit();
            jarPath = f.toString();
        } catch (IOException e) {
            throw new GenericKnimeSparkException(e);
        }

        final SparkJobCompiler compiler = new SparkJobCompiler();

        final String additionalImports = "";
        final String helperMethodsCode = "";
        final KnimeSparkJob job =
            compiler.addTransformationSparkJob2Jar("resources/knimeJobs.jar", jarPath, additionalImports,
                aTransformationCode, helperMethodsCode);

        //upload jar to job-server
        JobControler.uploadJobJar(jarPath);
        return job.getClass().getCanonicalName();
    }

}
