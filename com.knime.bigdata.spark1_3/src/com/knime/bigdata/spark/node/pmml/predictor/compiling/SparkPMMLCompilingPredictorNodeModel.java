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
 *   Created on 12.02.2015 by koetter
 */
package com.knime.bigdata.spark.node.pmml.predictor.compiling;

import javax.xml.transform.SourceLocator;

import org.knime.base.node.mine.util.PredictorHelper;
import org.knime.base.pmml.translation.PMMLTranslator;
import org.knime.base.pmml.translation.TerminatingMessageException;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.ext.sun.nodes.script.compile.CompilationFailedException;
import org.knime.ext.sun.nodes.script.compile.JavaCodeCompiler;

import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.node.pmml.predictor.PMMLPredictionTask;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.util.SparkIDs;
import com.knime.bigdata.spark.util.SparkPMMLUtil;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObject;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObjectSpec;

import net.sf.saxon.s9api.MessageListener;
import net.sf.saxon.s9api.XdmNode;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkPMMLCompilingPredictorNodeModel extends SparkNodeModel
implements MessageListener {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkPMMLCompilingPredictorNodeModel.class);
    /**The name of the java package.*/
    private static final String PACKAGE_NAME = "";
    /**The name of the java class.*/
    private static final String MODEL_NAME = "MainModel";

    private static final String CFG_KEY_OUTPROP = "outProp";

    private SettingsModelBoolean m_outputProbabilities = createOutputProbabilitiesSettingsModel();

    private SettingsModelBoolean m_changePredColName = PredictorHelper.getInstance().createChangePrediction();

    private SettingsModelString m_predColName = PredictorHelper.getInstance().createPredictionColumn();

    private SettingsModelString m_suffix = PredictorHelper.getInstance().createSuffix();

    /**
     *
     */
    public SparkPMMLCompilingPredictorNodeModel() {
        super(new PortType[]{PMMLPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * Creates the settings model that determines whether the node should output the probabilities for each class.
     * @return the settings model
     */
    static SettingsModelBoolean createOutputProbabilitiesSettingsModel() {
        return new SettingsModelBoolean(CFG_KEY_OUTPROP, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
//        final PMMLPortObjectSpec cms = (PMMLPortObjectSpec) inSpecs[0];
        return new PortObjectSpec[] {null};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final PMMLPortObject pmml = (PMMLPortObject)inObjects[0];
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[1];
        final String doc = pmml.getPMMLValue().toString();
        try {
            final String code = PMMLTranslator.generateJava(doc, this, PACKAGE_NAME, MODEL_NAME);
            CompiledModelPortObject cm = new CompiledModelPortObject(code, PACKAGE_NAME, MODEL_NAME, JavaCodeCompiler.JavaVersion.JAVA_7);
            final CompiledModelPortObjectSpec cms = (CompiledModelPortObjectSpec)cm.getSpec();
            final DataTableSpec resultSpec = SparkPMMLUtil.createPredictionResultSpec(data.getTableSpec(), cms,
                m_predColName.getStringValue(), m_outputProbabilities.getBooleanValue(), m_suffix.getStringValue());
            final String aOutputTableName = SparkIDs.createRDDID();
            final Integer[] colIdxs = SparkPMMLUtil.getColumnIndices(data.getTableSpec(), cms);
            final PMMLPredictionTask assignTask = new PMMLPredictionTask(m_outputProbabilities.getBooleanValue());
            assignTask.execute(exec, data.getData(), cm, colIdxs, aOutputTableName);
            return new PortObject[] {createSparkPortObject(data, resultSpec, aOutputTableName)};
        } catch (TerminatingMessageException tme) {
            throw new UnsupportedOperationException(tme.getMessage());
        } catch (CompilationFailedException e) {
            throw new InvalidSettingsException("The compilation of the generated code failed.\n" + e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void message(final XdmNode arg0, final boolean arg1, final SourceLocator arg2) {
        if (!arg1) {
            setWarningMessage(arg0.toString());
            LOGGER.warn(arg0.toString());
        } else {
            throw new TerminatingMessageException(arg0.toString());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_changePredColName.saveSettingsTo(settings);
        m_outputProbabilities.saveSettingsTo(settings);
        m_predColName.saveSettingsTo(settings);
        m_suffix.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        m_changePredColName.loadSettingsFrom(settings);
        m_outputProbabilities.loadSettingsFrom(settings);
        m_predColName.loadSettingsFrom(settings);
        m_suffix.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        m_changePredColName.validateSettings(settings);
        m_outputProbabilities.validateSettings(settings);
        m_predColName.validateSettings(settings);
        m_suffix.validateSettings(settings);
    }
}
