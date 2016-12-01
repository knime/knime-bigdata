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
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

import javax.swing.text.BadLocationException;
import javax.swing.text.Document;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.knime.base.node.jsnippet.guarded.GuardedDocument;
import org.knime.base.node.jsnippet.util.FlowVariableRepository;
import org.knime.base.node.jsnippet.util.JavaSnippetSettings;
import org.knime.base.node.jsnippet.util.ValidationReport;
import org.knime.base.node.jsnippet.util.field.InVar;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.FlowVariable.Type;
import org.knime.core.util.FileUtil;
import org.knime.core.util.Pair;
import org.knime.ext.sun.nodes.script.compile.CompilationFailedException;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextManager;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.jar.JarPacker;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.SparkContextProvider;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;
import com.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import com.knime.bigdata.spark.core.util.SparkIDs;
import com.knime.bigdata.spark.core.version.SparkVersion;
import com.knime.bigdata.spark.node.scripting.java.util.JavaSnippetJobInput;
import com.knime.bigdata.spark.node.scripting.java.util.JavaSnippetJobOutput;
import com.knime.bigdata.spark.node.scripting.java.util.SourceCompiler;
import com.knime.bigdata.spark.node.scripting.java.util.SparkJSnippet;
import com.knime.bigdata.spark.node.scripting.java.util.SparkJSnippetValidator;
import com.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelper;
import com.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelper.SnippetType;
import com.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelperRegistry;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public abstract class AbstractSparkJavaSnippetNodeModel extends SparkNodeModel {

    /**
     * The job id of all Spark Java snippet jobs
     */
    public static final String JOB_ID = "JavaSnippetJob";

    private final SnippetCompilationCache m_compilationCache = new SnippetCompilationCache();

    private final SnippetType m_snippetType;

    private SparkJSnippet m_sparkJavaSnippet;

    /**
     * This is a temporary container for settings during loading.
     * The actual settings must be obtained from {@link #m_sparkJavaSnippet},
     * which unfortunately we can only construct during/after configure()
     */
    private JavaSnippetSettings m_loadedSettings;

    private final UUID m_nodeModelInstanceUUID = UUID.randomUUID();

    /**
     * @param inPortTypes
     * @param outPortTypes
     * @param snippetType Type of the snippet node (e.g. source, sink, ...)
     */
    protected AbstractSparkJavaSnippetNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes,
        final SnippetType snippetType) {
        super(inPortTypes, outPortTypes);
        m_snippetType = snippetType;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        // ensure that the ingoing spark contexts are identical
        SparkContextID firstContextID = null;
        for (PortObjectSpec inSpec : inSpecs) {
            if (inSpec != null && inSpec instanceof SparkContextProvider) {
                SparkContextID currContextID = ((SparkContextProvider)inSpec).getContextID();
                if (firstContextID == null) {
                    firstContextID = currContextID;
                } else if (!firstContextID.equals(currContextID)) {
                    throw new InvalidSettingsException("Input objects belong to two different Spark contexts");
                }
            }
        }

        final SparkVersion sparkVersion = getSparkVersion(inSpecs);

        if (m_loadedSettings != null) {
            m_sparkJavaSnippet = new SparkJSnippet(sparkVersion, m_snippetType, m_loadedSettings);
            // throw this away to be safe (settings are managed by m_sparkJavaSnippet)
            m_loadedSettings = null;
        } else if (m_sparkJavaSnippet == null) {
            // initialize with default content
            m_sparkJavaSnippet = new SparkJSnippet(sparkVersion, m_snippetType);
        } else {
            m_sparkJavaSnippet.setSparkVersion(sparkVersion); // reinitialise (possible changed spark version)
        }
        validateSparkJavaSnippet();

        return null; // dummy value
    }

    /**
     * @throws InvalidSettingsException if validation failed (contains error message).
     */
    protected void validateSparkJavaSnippet() throws InvalidSettingsException {
        FlowVariableRepository flowVarRepository = new FlowVariableRepository(getAvailableInputFlowVariables());

        ValidationReport report = SparkJSnippetValidator.validate(flowVarRepository, m_sparkJavaSnippet);
        if (report.hasWarnings()) {
            setWarningMessage(StringUtils.join(report.getWarnings(), "\n"));
        }
        if (report.hasErrors()) {
            throw new InvalidSettingsException(StringUtils.join(report.getErrors(), "\n"));
        }

        // FIXME: do we need this?
        for (FlowVariable flowVar : flowVarRepository.getModified()) {
            if (flowVar.getType().equals(Type.INTEGER)) {
                pushFlowVariableInt(flowVar.getName(), flowVar.getIntValue());
            } else if (flowVar.getType().equals(Type.DOUBLE)) {
                pushFlowVariableDouble(flowVar.getName(), flowVar.getDoubleValue());
            } else {
                pushFlowVariableString(flowVar.getName(), flowVar.getStringValue());
            }
        }
    }

    /**
     * Method to be called by subclasses.
     * @param inData
     * @param createOutputObject Whether to produce an output {@link PortObject} or not
     * @param exec
     * @return an array containing zero or one {@link PortObject}, depending on whether is true or not
     * @throws Exception
     */
    protected PortObject[] executeSnippetJob(final PortObject[] inData, final boolean createOutputObject, final ExecutionContext exec) throws Exception {

        final String table1 = getRDDFromPortObjects(inData, 0);
        final String table2 = getRDDFromPortObjects(inData, 1);

        String namedOutputObject = null;
        if (createOutputObject) {
            namedOutputObject = SparkIDs.createRDDID();
        }

        final Pair<String, File> compiledSnippet = compileSnippetCached(getSparkVersion(inData));

        final JavaSnippetJobInput input = new JavaSnippetJobInput(table1, table2, namedOutputObject,
            compiledSnippet.getFirst(), getRequiredFlowVariables());

        final SparkContextID contextID = getContextID(inData);

        final JavaSnippetJobOutput output = SparkContextUtil.<JavaSnippetJobInput, JavaSnippetJobOutput>getJobWithFilesRunFactory(contextID, JOB_ID)
                .createRun(input, Arrays.asList(compiledSnippet.getSecond())).run(contextID, exec);

        PortObject[] toReturn = new PortObject[0];
        if(createOutputObject) {
            final DataTableSpec knimeOutputSpec = KNIMEToIntermediateConverterRegistry.convertSpec(output.getSpec(namedOutputObject));
            final SparkDataTable resultTable = new SparkDataTable(contextID, namedOutputObject, knimeOutputSpec);
            toReturn = new PortObject[]{new SparkDataPortObject(resultTable)};
        }

        return toReturn;
    }

    private String getRDDFromPortObjects(final PortObject[] inData, final int index) {
        String rddIDToReturn = null;

        if (inData != null && index < inData.length && inData[index] instanceof SparkDataPortObject) {
            rddIDToReturn = ((SparkDataPortObject)inData[index]).getTableName();
        }

        return rddIDToReturn;
    }

    /**
     * @param inData either the {@link PortObject} array in execute or the {@link PortObjectSpec} array in configure
     * @return the {@link SparkVersion} of the first Spark Context found in inData
     * @throws InvalidSettingsException
     */
    protected SparkVersion getSparkVersion(final Object[] inData) throws InvalidSettingsException {
        return SparkContextUtil.getSparkVersion(getContextID(inData));
    }

    /**
     * @param inData either the {@link PortObject} array in execute or the {@link PortObjectSpec} array in configure
     * @return the first {@link SparkContextID} found in inData
     * @throws InvalidSettingsException
     */
    protected SparkContextID getContextID(final Object[] inData) throws InvalidSettingsException {
        if (inData != null) {
            for (Object in : inData) {
                if (in != null && in instanceof SparkContextProvider) {
                    return ((SparkContextProvider)in).getContextID();
                }
            }
        }

        return SparkContextManager.getDefaultSparkContextID();
    }

    private Pair<String, File> compileSnippetCached(final SparkVersion sparkVersion)
            throws BadLocationException, ClassNotFoundException, CompilationFailedException, IOException {

        m_sparkJavaSnippet.setSparkVersion(sparkVersion); // reinitialise (possible changed spark version)
        final String classHash = createClassHash();

        if (!m_compilationCache.isCached(classHash)) {
            final JavaSnippetSettings settings = m_sparkJavaSnippet.getSettings();

            final JavaSnippetHelper helper = JavaSnippetHelperRegistry.getInstance().getHelper(sparkVersion);
            // create a copy of the actual document so that we can insert the hash as class name suffix without
            // changing the class name in the user visible editor
            final GuardedDocument docClone =
                helper.createGuardedSnippetDocument(m_snippetType, settings);
            helper.updateGuardedClassnameSuffix(m_snippetType, docClone, settings.getJavaSnippetFields(), classHash);

            final SourceCompiler compiler = new SourceCompiler(helper.getSnippetClassName(m_snippetType, classHash),
                docClone.getText(0, docClone.getLength()),
                m_sparkJavaSnippet.getClassPath());
            m_compilationCache.updateCache(classHash, compiler.getClassName(), createSnippetJarFile(compiler));
        }

        return m_compilationCache.getCachedCompiledSnippet();
    }

    private String createClassHash() throws BadLocationException {
        final Document codeDoc = m_sparkJavaSnippet.getDocument();

        // adding the UUID to the code (as a comment) makes the code unique to this node model instance
        // otherwise this can lead to strange side effects between distinct Spark Java snippet nodes that
        // compile the same code
        final String code =
            codeDoc.getText(0, codeDoc.getLength()) + String.format("// %s", m_nodeModelInstanceUUID.toString());
        return DigestUtils.sha256Hex(code);
    }

    private File createSnippetJarFile(final SourceCompiler compiler) throws IOException {
        final File snippetDir = new File(KNIMEConstants.getKNIMEHomeDir(), "sparkSnippetJars");
        if (!snippetDir.exists()) {
            snippetDir.mkdirs();
        }

        final File jarFile = FileUtil.createTempFile(compiler.getClassName(), ".jar", snippetDir, true);
        JarPacker.createJar(jarFile, "", compiler.getBytecode());
        return jarFile;
    }

    private HashMap<String, Object> getRequiredFlowVariables() {

        final FlowVariableRepository flowVarRepo = new FlowVariableRepository(getAvailableInputFlowVariables());
        final HashMap<String, Object> requiredFlowVars = new HashMap<>();

        // use settings directly
        for (InVar inCol : m_sparkJavaSnippet.getSettings().getJavaSnippetFields().getInVarFields()) {
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
        if (m_loadedSettings != null) {
            m_loadedSettings.saveSettings(settings);
        } else if (m_sparkJavaSnippet != null) {
            m_sparkJavaSnippet.getSettings().saveSettings(settings);
        }
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
        if (m_loadedSettings == null) {
            m_loadedSettings = new JavaSnippetSettings();
        }

        m_loadedSettings.loadSettings(settings);
        m_sparkJavaSnippet = null;
    }

}