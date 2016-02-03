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

import java.util.Map;
import java.util.UUID;

import javax.swing.text.BadLocationException;
import javax.swing.text.Position;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.types.StructType;
import org.knime.base.node.jsnippet.guarded.GuardedDocument;
import org.knime.base.node.jsnippet.guarded.GuardedSection;
import org.knime.base.node.jsnippet.guarded.JavaSnippetDocument;
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
        final SourceCompiler compiler = compileSnippet();

        final SparkJavaSnippetTask task = new SparkJavaSnippetTask(context, table1, table2, compiler.getBytecode(),
            compiler.getClassName(), resultRDDName);

        //TODO: Provide the input table spec as StructType within the JavaSnippet node see StructTypeBuilder as
        //reference and use the SparkTypeConverter interface for the conversion from spec to struct

        JobResult result = task.execute(exec);
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

    private SourceCompiler compileSnippet() throws BadLocationException, GenericKnimeSparkException {
        final String code = insertFieldValues(m_snippet, getAvailableInputFlowVariables());

        final String newClassName = m_snippet.getClassName() + "_" + UUID.randomUUID().toString().replace("-", "");
        final String newCode = code.replace(String.format("public class %s extends ", m_snippet.getClassName()),
            String.format("public class %s extends ", newClassName));

        try {
            return new SourceCompiler(newClassName, newCode);
        } catch (ClassNotFoundException | CompilationFailedException e) {
            LOGGER.error(e.getMessage());
            throw new GenericKnimeSparkException(e);
        }
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