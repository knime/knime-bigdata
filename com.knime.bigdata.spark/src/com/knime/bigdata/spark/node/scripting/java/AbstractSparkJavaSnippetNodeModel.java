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
 *   Created on 24.06.2015 by koetter
 */
package com.knime.bigdata.spark.node.scripting.java;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.swing.text.BadLocationException;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;
import org.knime.base.node.jsnippet.guarded.GuardedDocument;
import org.knime.base.node.jsnippet.util.FlowVariableRepository;
import org.knime.base.node.jsnippet.util.ValidationReport;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.FlowVariable.Type;

import com.knime.bigdata.spark.SparkPlugin;
import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.client.jar.SparkJobCompiler;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.node.AbstractSparkNodeModel;
import com.knime.bigdata.spark.node.scripting.util.SparkJavaSnippet;
import com.knime.bigdata.spark.node.scripting.util.SparkJavaSnippetSettings;
import com.knime.bigdata.spark.port.data.AbstractSparkRDD;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDGenerator;

/**
 *
 * @author koetter
 */
public abstract class AbstractSparkJavaSnippetNodeModel extends AbstractSparkNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(AbstractSparkJavaSnippetNodeModel.class);

    private final SparkJavaSnippetSettings m_settings;
    private final SparkJavaSnippet m_snippet;

    /**
         *
         * TODO - move into separate class ..... and fix types
         * @author dwk
         */
        protected static class KnimeDataTypeFromSqlDataType {
            public static final Map<org.apache.spark.sql.api.java.DataType, DataType> DATA_TYPES_BY_CLASS = new HashMap<>();
            static {
              DATA_TYPES_BY_CLASS.put(org.apache.spark.sql.api.java.DataType.BooleanType, BooleanCell.TYPE );
              DATA_TYPES_BY_CLASS.put(org.apache.spark.sql.api.java.DataType.ByteType, IntCell.TYPE);
              DATA_TYPES_BY_CLASS.put(org.apache.spark.sql.api.java.DataType.ShortType, IntCell.TYPE);
              DATA_TYPES_BY_CLASS.put(org.apache.spark.sql.api.java.DataType.IntegerType, IntCell.TYPE);
              DATA_TYPES_BY_CLASS.put(org.apache.spark.sql.api.java.DataType.LongType, LongCell.TYPE);
              DATA_TYPES_BY_CLASS.put(org.apache.spark.sql.api.java.DataType.FloatType, DoubleCell.TYPE);
              DATA_TYPES_BY_CLASS.put(org.apache.spark.sql.api.java.DataType.DoubleType, DoubleCell.TYPE);
              DATA_TYPES_BY_CLASS.put(org.apache.spark.sql.api.java.DataType.DateType, StringCell.TYPE);
              DATA_TYPES_BY_CLASS.put(org.apache.spark.sql.api.java.DataType.StringType, StringCell.TYPE);
            }

            /**
             * @param aSqlDataType
             * @return corresponding KNIME data type
             */
            public static DataType get(final org.apache.spark.sql.api.java.DataType aSqlDataType) {
                return DATA_TYPES_BY_CLASS.get(aSqlDataType);
            }

        }

    public static class SqlDataTypeFromKnimeDataType {
        public static final Map<DataType, org.apache.spark.sql.api.java.DataType> DATA_TYPES_BY_CLASS = new HashMap<>();
        static {
          DATA_TYPES_BY_CLASS.put(BooleanCell.TYPE, org.apache.spark.sql.api.java.DataType.BooleanType);
          DATA_TYPES_BY_CLASS.put(IntCell.TYPE, org.apache.spark.sql.api.java.DataType.ByteType);
          DATA_TYPES_BY_CLASS.put(IntCell.TYPE, org.apache.spark.sql.api.java.DataType.ShortType);
          DATA_TYPES_BY_CLASS.put(IntCell.TYPE, org.apache.spark.sql.api.java.DataType.IntegerType);
          DATA_TYPES_BY_CLASS.put(LongCell.TYPE, org.apache.spark.sql.api.java.DataType.LongType);
          DATA_TYPES_BY_CLASS.put(DoubleCell.TYPE, org.apache.spark.sql.api.java.DataType.FloatType);
          DATA_TYPES_BY_CLASS.put(DoubleCell.TYPE, org.apache.spark.sql.api.java.DataType.DoubleType);
          DATA_TYPES_BY_CLASS.put(StringCell.TYPE, org.apache.spark.sql.api.java.DataType.DateType);
          DATA_TYPES_BY_CLASS.put(StringCell.TYPE, org.apache.spark.sql.api.java.DataType.StringType);
        }

        /**
         * @return corresponding KNIME data type
         */
        public static org.apache.spark.sql.api.java.DataType get(final DataType aKNIMEDataType) {
            return DATA_TYPES_BY_CLASS.get(aKNIMEDataType);
        }

    }

    /**
     * @param inPortTypes
     * @param outPortTypes
     * @param snippet
     */
    public AbstractSparkJavaSnippetNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes,
        final SparkJavaSnippet snippet, final String defaultContent) {
        super(inPortTypes, outPortTypes);
        m_settings = new SparkJavaSnippetSettings(defaultContent);
        m_snippet = snippet;
//      m_snippet.attachLogger(LOGGER);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        m_snippet.setSettings(m_settings);
        FlowVariableRepository flowVarRepository =
            new FlowVariableRepository(getAvailableInputFlowVariables());
        ValidationReport report = m_snippet.validateSettings(inSpecs,
                flowVarRepository);
        if (report.hasWarnings()) {
            setWarningMessage(StringUtils.join(report.getWarnings(), "\n"));
        }
        if (report.hasErrors()) {
            throw new InvalidSettingsException(
                    StringUtils.join(report.getErrors(), "\n"));
        }
        DataTableSpec outSpec = m_snippet.configure(inSpecs,
                flowVarRepository);
        for (FlowVariable flowVar : flowVarRepository.getModified()) {
            if (flowVar.getType().equals(Type.INTEGER)) {
                pushFlowVariableInt(flowVar.getName(), flowVar.getIntValue());
            } else if (flowVar.getType().equals(Type.DOUBLE)) {
                pushFlowVariableDouble(flowVar.getName(),
                        flowVar.getDoubleValue());
            } else {
                pushFlowVariableString(flowVar.getName(),
                        flowVar.getStringValue());
            }
        }
        return new DataTableSpec[] {outSpec};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        m_snippet.setSettings(m_settings);
        final SparkDataPortObject data1;
        final SparkDataPortObject data2;
        if (inData == null || inData.length == 0) {
            data1 = null;
            data2 = null;
        } else if (inData.length == 1){
            data1 = (SparkDataPortObject)inData[0];
            data2 = null;
        } else {
            data1 = (SparkDataPortObject)inData[0];
            data2 = (SparkDataPortObject)inData[1];
        }

        final String context;
        final String table1Name;
        final AbstractSparkRDD table1;
        if (data1 != null) {
            table1 = data1.getData();
            context = table1.getContext();
            table1Name = table1.getID();
        } else {
            table1 = null;
            context = KnimeContext.getSparkContext();
            table1Name = null;
        }
        final String table2Name;
        final SparkDataTable table2;
        if (data2 != null) {
            //we have two incoming RDDs
            table2 = data2.getData();
            table2Name = table2.getID();
        } else {
            table2 = null;
            table2Name = null;
        }
        if (table1 != null && table2 != null && !table1.compatible(table2)) {
            throw new InvalidSettingsException("Input objects belong to two different Spark contexts");
        }
        String tableName = SparkIDGenerator.createID();
        //now compile code, add to jar and upload jar:
        final KnimeSparkJob job = addJob2Jar(m_snippet);
//        String jobDescription = "return aInput1;";
//        final KnimeSparkJob job = addTransformationJob2Jar(jobDescription);

        //call the Spark job with the two rdds and use the tableName as id for the result RDD and
        //the job description as source code for the job

        //start job with proper parameters
        final String jobId = JobControler.startJob(context, job, params2Json(table1Name, table2Name, tableName));
        final JobResult result = JobControler.waitForJobAndFetchResult(jobId, exec);
        final StructType tableTypes = result.getTables().get(tableName);
        if (tableTypes != null) {
            final List<DataColumnSpec> specs = new LinkedList<>();
            final DataColumnSpecCreator specCreator = new DataColumnSpecCreator("Test", StringCell.TYPE);
            for (StructField field : tableTypes.getFields()) {
                specCreator.setName(field.getName());
                specCreator.setType(KnimeDataTypeFromSqlDataType.get(field.getDataType()));
                specs.add(specCreator.createSpec());
            }
            final DataTableSpec resultSpec = new DataTableSpec(specs.toArray(new DataColumnSpec[0]));
            SparkDataTable resultTable = new SparkDataTable(context, tableName, resultSpec);
            final SparkDataPortObject resultObject = new SparkDataPortObject(resultTable);
            return new PortObject[]{resultObject};
        } else {
            return new PortObject[] {};
        }
    }

    private final String params2Json(@Nonnull final String aInputTable1, final String aInputTable2, @Nonnull final String aOutputTable) {
        final List<String> inputParams = new LinkedList<>();
        if (aInputTable1 != null) {
            inputParams.add(ParameterConstants.PARAM_TABLE_1);
            inputParams.add(aInputTable1);
        }
        if (aInputTable2 != null) {
            inputParams.add(ParameterConstants.PARAM_TABLE_2);
            inputParams.add(aInputTable2);
        }
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParams.toArray(new String[0]),
            ParameterConstants.PARAM_OUTPUT, new String[]{ParameterConstants.PARAM_TABLE_1, aOutputTable}});
    }

    private KnimeSparkJob addJob2Jar(final SparkJavaSnippet snippet) throws GenericKnimeSparkException, BadLocationException {
        final GuardedDocument codeDoc = m_snippet.getDocument();
        final String code = codeDoc.getText(0, codeDoc.getLength());
        final String jarPath;
        try {
            File f = File.createTempFile("knimeJobUtils", ".jar");
            f.deleteOnExit();
            jarPath = f.toString();
        } catch (IOException e) {
            throw new GenericKnimeSparkException(e);
        }

        //TK_TODO: Use the SparkJavaSnippetCompiler instead of the SparkJobCompiler
        final SparkJobCompiler compiler = new SparkJobCompiler();
        final String root = SparkPlugin.getDefault().getPluginRootPath();
        //do not overwrite the knimeJobs.jar but create a unique jar each time or a unique jar per session to prevent
        //that multiple users overwrite the job jars while jobs are still running
        final KnimeSparkJob job =
            compiler.addSparkJob2Jar(root + "/resources/knimeJobs.jar", jarPath, code, m_snippet.getClassName());

        //upload jar to job-server
        JobControler.uploadJobJar(jarPath);
        return job;
    }

    private KnimeSparkJob addTransformationJob2Jar(final String aTransformationCode) throws GenericKnimeSparkException {

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

        final String root = SparkPlugin.getDefault().getPluginRootPath();

        final KnimeSparkJob job =
            compiler.addTransformationSparkJob2Jar(root + "/resources/knimeJobs.jar", jarPath, additionalImports,
                aTransformationCode, helperMethodsCode);

        //upload jar to job-server
        JobControler.uploadJobJar(jarPath);
        return job;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        SparkJavaSnippetSettings s = new SparkJavaSnippetSettings();
        s.loadSettings(settings);
        // TODO: Check settings
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettings(settings);
    }

}