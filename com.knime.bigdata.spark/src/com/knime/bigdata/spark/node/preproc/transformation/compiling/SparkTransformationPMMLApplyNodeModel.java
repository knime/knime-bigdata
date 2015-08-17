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
 *   Created on 31.07.2015 by dwk
 */
package com.knime.bigdata.spark.node.preproc.transformation.compiling;

import java.io.IOException;

import javax.xml.transform.SourceLocator;

import net.sf.saxon.s9api.MessageListener;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XdmNode;

import org.knime.base.pmml.translation.PMMLTranslator;
import org.knime.base.pmml.translation.TerminatingMessageException;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.ext.sun.nodes.script.compile.CompilationFailedException;

import com.knime.bigdata.spark.node.AbstractSparkNodeModel;
import com.knime.bigdata.spark.node.mllib.pmml.predictor.PMMLAssignTask;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDs;
import com.knime.bigdata.spark.util.SparkUtil;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObject;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObjectSpec;

/**
 * The PMML transformation node model.
 *
 * @author koetter
 */
public class SparkTransformationPMMLApplyNodeModel extends AbstractSparkNodeModel implements MessageListener {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkTransformationPMMLApplyNodeModel.class);
    /**The name of the java package.*/
    private static final String PACKAGE_NAME = "";
    /**The name of the java class.*/
    private static final String MODEL_NAME = "MainModel";

    /**
     * Constructor.
     */
    public SparkTransformationPMMLApplyNodeModel() {
        super(new PortType[]{PMMLPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        //TODO: Get the result spec based on the input PMML
//        final PMMLPortObjectSpec pmmlSpec = (PMMLPortObjectSpec) inSpecs[0];
//        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[1];
//        DataTableSpec resultSpec = createResultSpec(sparkSpec.getTableSpec(), new CompiledMpmmlSpec);
        return new PortObjectSpec[] {null};
    }

    private DataTableSpec createResultSpec(final DataTableSpec inSpec, final CompiledModelPortObjectSpec cms) {
        final DataColumnSpec[] specs = cms.getTransformationsResultColSpecs(inSpec);
        return new DataTableSpec(inSpec, new DataTableSpec(specs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final PMMLPortObject pmml = (PMMLPortObject)inObjects[0];
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[1];
        final String aOutputTableName = SparkIDs.createRDDID();
        exec.setMessage("Compile incoming PMML model");
        final CompiledModelPortObject cpmml = compileModel(pmml);
        final CompiledModelPortObjectSpec cms = (CompiledModelPortObjectSpec)cpmml.getSpec();
        final DataTableSpec resultSpec = createResultSpec(data.getTableSpec(), cms);
        final SparkDataTable resultRDD = new SparkDataTable(data.getContext(), aOutputTableName, resultSpec);
        final Integer[] colIdxs = SparkUtil.getColumnIndices(data.getTableSpec(), cpmml.getModel());
        final PMMLAssignTask assignTask = new PMMLAssignTask();
        exec.setMessage("Execute Spark job");
        exec.checkCanceled();
        assignTask.execute(exec, data.getData(), cpmml, colIdxs, true, resultRDD);
        return new PortObject[] {new SparkDataPortObject(resultRDD)};
    }

    private CompiledModelPortObject compileModel(final PMMLPortObject pmml)
            throws ClassNotFoundException, IOException, SaxonApiException, InvalidSettingsException {
        String doc = pmml.getPMMLValue().toString();
        String code;
        try {
            code = PMMLTranslator.generateJava(doc, this, PACKAGE_NAME, MODEL_NAME);
        } catch (TerminatingMessageException tme) {
            throw new UnsupportedOperationException(tme.getMessage());
        }

        try {
            return new CompiledModelPortObject(code, PACKAGE_NAME, MODEL_NAME);
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
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
    }
}
