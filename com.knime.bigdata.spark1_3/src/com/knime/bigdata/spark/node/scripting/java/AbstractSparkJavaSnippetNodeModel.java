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

import java.util.HashMap;
import java.util.UUID;

import javax.swing.text.BadLocationException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.types.StructType;
import org.knime.base.node.jsnippet.guarded.GuardedDocument;
import org.knime.base.node.jsnippet.util.FlowVariableRepository;
import org.knime.base.node.jsnippet.util.JavaField.InVar;
import org.knime.base.node.jsnippet.util.JavaSnippetSettings;
import org.knime.base.node.jsnippet.util.ValidationReport;
import org.knime.core.data.DataTableSpec;
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
import org.knime.ext.sun.nodes.script.compile.CompilationFailedException;

import com.knime.bigdata.spark.jobserver.client.jar.SourceCompiler;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.node.scripting.java.util.SparkJavaSnippet;
import com.knime.bigdata.spark.node.scripting.java.util.SparkJavaSnippetJarCacheInfo;
import com.knime.bigdata.spark.node.scripting.java.util.SparkJavaSnippetTask;
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

    private final JavaSnippetSettings m_settings;

    private final SparkJavaSnippet m_snippet;

    private SourceCompiler cachedSnippetCompilation;

    private SparkJavaSnippetJarCacheInfo jarCacheInfo;

    private final UUID nodeModelInstanceUUID = UUID.randomUUID();

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

        final AbstractSparkRDD table1 = getRDDFromPortObjects(inData, 0);
        final AbstractSparkRDD table2 = getRDDFromPortObjects(inData, 1);

        if (table1 != null && table2 != null && !table1.compatible(table2)) {
            throw new InvalidSettingsException("Input objects belong to two different Spark contexts");
        }

        final KNIMESparkContext context = getContext(inData);
        final String resultRDDName = SparkIDs.createRDDID();

        final SourceCompiler compiler = compileSnippetCached();

        final SparkJavaSnippetTask task = new SparkJavaSnippetTask(context, table1, table2, compiler.getBytecode(),
            compiler.getClassName(), resultRDDName, jarCacheInfo, getRequiredFlowVariables());

        //TODO: Provide the input table spec as StructType within the JavaSnippet node see StructTypeBuilder as
        //reference and use the SparkTypeConverter interface for the conversion from spec to struct

        JobResult result = task.execute(exec);

        jarCacheInfo = task.getJarCacheInfo();

        final StructType tableStructure = result.getTableStructType(resultRDDName);
        if (tableStructure != null) {
            final DataTableSpec resultSpec = SparkUtil.toTableSpec(tableStructure);
            final SparkDataTable resultTable = new SparkDataTable(context, resultRDDName, resultSpec);
            final SparkDataPortObject resultObject = new SparkDataPortObject(resultTable);
            return new PortObject[]{resultObject};
        } else {
            return new PortObject[] {};
        }
    }

    private AbstractSparkRDD getRDDFromPortObjects(final PortObject[] inData, final int index) {
        if (inData == null || index >= inData.length || inData[index] == null) {
            return null;
        } else {
            return ((SparkDataPortObject) inData[index]).getData();
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

    private SourceCompiler compileSnippetCached() throws BadLocationException, GenericKnimeSparkException {
        final GuardedDocument codeDoc = m_snippet.getDocument();

        // adding the UUID to the code (as a comment) makes the code unique to this node model instance
        // otherwise this can lead to strange side effects between distinct Spark Java snippet nodes that
        // compile the same code
        final String code = codeDoc.getText(0, codeDoc.getLength()) + String.format("// %s", nodeModelInstanceUUID.toString());
        final String newClassName = createClassNameWithHash(code);

        if (cachedSnippetCompilation == null || !cachedSnippetCompilation.getClassName().equals(newClassName)) {
            final String newCode = code.replace(String.format("public class %s extends ", m_snippet.getClassName()),
                String.format("public class %s extends ", newClassName));

            try {
                cachedSnippetCompilation = new SourceCompiler(newClassName, newCode);
            } catch (ClassNotFoundException | CompilationFailedException e) {
                LOGGER.error(e.getMessage());
                throw new GenericKnimeSparkException(e);
            }
        }

        return cachedSnippetCompilation;
    }

    private String createClassNameWithHash(final String code) {
        return m_snippet.getClassName() + "_" + DigestUtils.sha256Hex(code);
    }

    private HashMap<String, Object> getRequiredFlowVariables() throws GenericKnimeSparkException {

        final FlowVariableRepository flowVarRepo = new FlowVariableRepository(getAvailableInputFlowVariables());
        final HashMap<String, Object> requiredFlowVars = new HashMap<String, Object>();

        for (InVar inCol : m_snippet.getSystemFields().getInVarFields()) {
            String javaFieldName = inCol.getJavaName();
            Object javaFieldValue = flowVarRepo.getValueOfType(inCol.getKnimeName(), inCol.getJavaType());

            if (javaFieldValue != null) {
                requiredFlowVars.put(javaFieldName, javaFieldValue);
            }
        }

        return requiredFlowVars;
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