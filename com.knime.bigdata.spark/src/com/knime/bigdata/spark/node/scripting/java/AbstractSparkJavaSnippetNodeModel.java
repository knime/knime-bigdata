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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.swing.text.BadLocationException;
import javax.swing.text.Position;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.StructType;
import org.knime.base.node.jsnippet.guarded.GuardedDocument;
import org.knime.base.node.jsnippet.guarded.GuardedSection;
import org.knime.base.node.jsnippet.guarded.JavaSnippetDocument;
import org.knime.base.node.jsnippet.util.FlowVariableRepository;
import org.knime.base.node.jsnippet.util.JavaSnippetSettings;
import org.knime.base.node.jsnippet.util.ValidationReport;
import org.knime.base.node.jsnippet.util.field.InVar;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.config.Config;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.FlowVariable.Type;
import org.knime.core.util.FileUtil;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.jar.JarPacker;
import com.knime.bigdata.spark.jobserver.client.jar.SparkJobCompiler;
import com.knime.bigdata.spark.jobserver.client.jar.SparkJobCompiler.SourceCompiler;
import com.knime.bigdata.spark.jobserver.jobs.AbstractSparkJavaSnippet;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.node.scripting.java.util.SparkJavaSnippet;
import com.knime.bigdata.spark.port.SparkContextProvider;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.AbstractSparkRDD;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDs;
import com.knime.bigdata.spark.util.SparkUtil;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public abstract class AbstractSparkJavaSnippetNodeModel extends SparkNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(AbstractSparkJavaSnippetNodeModel.class);

    private static final String CLASS_PREFIX = "SJS";
    private static final String CFG_FILE_NAME = "snippet.xml";
    private static final String CFG_CLASS_NAMES = "classNames";
    //Use an empty package path for now since the start job method will not find the class if we add a package name
    private static final String PACKAGE_PATH = "";
    private static final File SNIPPET_FILE = getSnippetFile();
    private final JavaSnippetSettings m_settings;
    private final SparkJavaSnippet m_snippet;
    private String[] m_classNames;


    /**
     * @param inPortTypes
     * @param outPortTypes
     * @param snippet
     * @param defaultContent
     */
    protected AbstractSparkJavaSnippetNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes,
        final SparkJavaSnippet snippet, final String defaultContent) {
        super(inPortTypes, outPortTypes);
        m_settings = new JavaSnippetSettings(defaultContent);
        m_snippet = snippet;
    }

    /**
     * @param inSpecs the input {@link PortObjectSpec} array
     * @return the result {@link DataTableSpec}
     * @throws InvalidSettingsException
     */
    protected DataTableSpec getResultSpec(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        m_snippet.setSettings(m_settings);
        FlowVariableRepository flowVarRepository =
            new FlowVariableRepository(getAvailableInputFlowVariables());
        ValidationReport report = m_snippet.validateSettings(inSpecs, flowVarRepository);
        if (report.hasWarnings()) {
            setWarningMessage(StringUtils.join(report.getWarnings(), "\n"));
        }
        if (report.hasErrors()) {
            throw new InvalidSettingsException(
                    StringUtils.join(report.getErrors(), "\n"));
        }
        final DataTableSpec outSpec = m_snippet.configure(inSpecs, flowVarRepository);
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
        return outSpec;
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
        final KNIMESparkContext context = getContext(inData);
        final String table1Name;
        final AbstractSparkRDD table1;
        if (data1 != null) {
            table1 = data1.getData();
            table1Name = table1.getID();
        } else {
            table1 = null;
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
        final String tableName = SparkIDs.createRDDID();
        final KnimeSparkJob job = addJob2Jar(context, m_snippet);
        final String config = params2Json(table1Name, table2Name, tableName);
        //TODO: Provide the input table spec as StructType within the JavaSnippet node see StructTypeBuilder as
        //reference and use the SparkTypeConverter interface for the conversion from spec to struct
        final String jobId = JobControler.startJob(context, job, config);
        final JobResult result = JobControler.waitForJobAndFetchResult(context, jobId, exec);
        final StructType tableStructure = result.getTableStructType(tableName);
        if (tableStructure != null) {
            final DataTableSpec resultSpec = SparkUtil.toTableSpec(tableStructure);
            final SparkDataTable resultTable = new SparkDataTable(context, tableName, resultSpec);
            final SparkDataPortObject resultObject = new SparkDataPortObject(resultTable);
            return new PortObject[]{resultObject};
        } else {
            return new PortObject[] {};
        }
    }

    /**
     * @param inData either the {@link PortObject} array in execute or the {@link PortObjectSpec} array in configure
     * @return the {@link KNIMESparkContext} to use
     * @throws InvalidSettingsException
     */
    protected KNIMESparkContext getContext(final Object[] inData) throws InvalidSettingsException {
        if (inData != null && inData.length >= 1 && (inData[0] instanceof SparkContextProvider)) {
            return ((SparkContextProvider)inData[0]).getContext();
        }
        throw new InvalidSettingsException("No input RDD available");
    }

    private final String params2Json(@Nonnull final String aInputTable1, final String aInputTable2,
        @Nonnull final String aOutputTable) {
        final List<String> inputParams = new LinkedList<>();
        if (aInputTable1 != null) {
            inputParams.add(AbstractSparkJavaSnippet.PARAM_INPUT_TABLE_KEY1);
            inputParams.add(aInputTable1);
        }
        if (aInputTable2 != null) {
            inputParams.add(AbstractSparkJavaSnippet.PARAM_INPUT_TABLE_KEY2);
            inputParams.add(aInputTable2);
        }
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT, inputParams.toArray(new String[0]),
            ParameterConstants.PARAM_OUTPUT,
            new String[]{AbstractSparkJavaSnippet.PARAM_OUTPUT_TABLE_KEY, aOutputTable}});
    }

    private KnimeSparkJob addJob2Jar(final KNIMESparkContext context, final SparkJavaSnippet snippet)
            throws Exception {
        final String code = insertFieldValues(m_snippet, getAvailableInputFlowVariables());
        final SparkJobCompiler compiler = new SparkJobCompiler();
        final String newClassName = compiler.generateUniqueClassName(CLASS_PREFIX);
        final String newCode = code.replace("class " + m_snippet.getClassName() + " ", "class " + newClassName + " ");
        final SourceCompiler compiledJob = compiler.compileAndCreateInstance(newClassName, newCode);
        final Map<String, byte[]> classMap = compiledJob.getBytecode();
        m_classNames = classMap.keySet().toArray(new String[0]);
        synchronized (SNIPPET_FILE) {
            try {
                final File tempFile = FileUtil.createTempFile("SJS", ".jar", SNIPPET_FILE.getParentFile(), true);
                if (!SNIPPET_FILE.exists()) {
                    //create a new jar with the new snippet classes
                    JarPacker.createJar(tempFile, PACKAGE_PATH, classMap);
                } else {
                    //create a new file that will contain all previous classes plus the new snippet classes
                    JarPacker.add2Jar(SNIPPET_FILE.getPath(), tempFile.getPath(), PACKAGE_PATH, classMap);
                }
                //replace the old java snippet jar with the new one
                Files.move(tempFile.toPath(), SNIPPET_FILE.toPath(), StandardCopyOption.REPLACE_EXISTING);
                //merge the java snippet file and the regular KNIME job classes
                final File mergedFile = File.createTempFile("MSJS", ".jar", SNIPPET_FILE.getParentFile());
                mergedFile.deleteOnExit();
                JarPacker.mergeJars(SNIPPET_FILE.getPath(), SparkUtil.getJobJarPath(), mergedFile);
                //upload the new job jar to the server
                JobControler.uploadJobJar(context, mergedFile.getPath());
                mergedFile.delete();
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
                throw new GenericKnimeSparkException(e);
            }
        }
        final KnimeSparkJob job = compiledJob.getInstance();
        return job;
    }

    private static String insertFieldValues(final SparkJavaSnippet snippet, final Map<String, FlowVariable> flowVars)
            throws BadLocationException {
        //TODO: Provide the flow variables as key value map in the config and assign the values of the variable to
        // their corresponding members in the Spark job class
        final GuardedDocument codeDoc = snippet.getDocument();
        // populate the system input flow variable fields with data
        final FlowVariableRepository flowVarRepo = new FlowVariableRepository(flowVars);
        //get the guarded flow variable fields section
        final GuardedSection fieldsSection = codeDoc.getGuardedSection(JavaSnippetDocument.GUARDED_FIELDS);
        String fieldsText = fieldsSection.getText();
        for (final InVar inCol : snippet.getSystemFields().getInVarFields()) {
            final Class<?> javaType = inCol.getJavaType();
            final FlowVariable flowVariable = flowVarRepo.getFlowVariable(inCol.getKnimeName());
            final Object v = flowVarRepo.getValueOfType(inCol.getKnimeName(), javaType);
            final String valueString;
            if (v == null) {
                valueString = "null";
            } else {
                final Type type = flowVariable.getType();
                switch (type) {
                    case DOUBLE:
                        valueString = ((Double)v).toString();
                        break;
                    case INTEGER:
                        valueString = ((Integer)v).toString();
                        break;
                    default:
                        //this is the String and fallback case
                        valueString = "\"" + StringEscapeUtils.escapeJava(v.toString())  + "\"";
                        break;
                }
            }
            final String fieldName = inCol.getJavaName();
            fieldsText = fieldsText.replace(fieldName + ";", fieldName + " = " + valueString + ";");
        }
        final String origCode = codeDoc.getText(0, codeDoc.getLength());
        final Position start = fieldsSection.getStart();
        final Position end = fieldsSection.getEnd();
        final StringBuilder buf = new StringBuilder(origCode.substring(0, start.getOffset()));
        buf.append(fieldsText);
        buf.append(origCode.substring(end.getOffset() + 1));
        return buf.toString();
    }

    /**
     * @return
     */
    private static File getSnippetFile() {
        final String appID = SparkIDs.getSparkApplicationID();
        final File sparkSnippetDir = new File(KNIMEConstants.getKNIMEHomeDir(), "sparkSnippetJars");
        if (!sparkSnippetDir.exists()) {
            sparkSnippetDir.mkdirs();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Creates snippet jar directory: " + sparkSnippetDir.getPath());
            }
        }
        final File sparkJavaSnippetJar = new File(sparkSnippetDir, "j_"+ appID + ".jar");
        return sparkJavaSnippetJar;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void resetInternal() {
        if (m_classNames != null && m_classNames.length > 0) {
            //remove the generated classes from the jar file
            try {
                final Set<String> entryNames = new HashSet<>(m_classNames.length);
                for (int i = 0, length = m_classNames.length; i < length; i++) {
                    final String packagePath =
                            PACKAGE_PATH == null || PACKAGE_PATH.isEmpty() ? "" : PACKAGE_PATH + "/";
                    entryNames.add(packagePath + m_classNames[i] + ".class");
                }
                synchronized (SNIPPET_FILE) {
                    JarPacker.removeFromJar(SNIPPET_FILE, entryNames);
                }
            } catch (IOException e) {
                LOGGER.warn("Exception while removeing class from Spark Snipper jar: " + e.getMessage(), e);
            }
            m_classNames = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
        CanceledExecutionException {
        final File settingFile = new File(nodeInternDir, CFG_FILE_NAME);
        try(final FileInputStream inData = new FileInputStream(settingFile)){
            final ConfigRO config = NodeSettings.loadFromXML(inData);
            m_classNames = config.getStringArray(CFG_CLASS_NAMES);

        } catch (final InvalidSettingsException | RuntimeException e) {
            throw new IOException("Failed to load snippet settings", e.getCause());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
        CanceledExecutionException {
        final File settingFile = new File(nodeInternDir, CFG_FILE_NAME);
        try (final FileOutputStream dataOS = new FileOutputStream(settingFile)){
            final Config config = new NodeSettings("saveInternalsSettings");
            config.addStringArray(CFG_CLASS_NAMES, m_classNames);
            config.saveToXML(dataOS);
        } catch (final Exception e) {
            throw new IOException(e.getMessage(), e.getCause());
        }
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
        final JavaSnippetSettings s = new JavaSnippetSettings();
        s.loadSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettings(settings);
    }

}