/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
package org.knime.bigdata.spark.node.pmml.transformation.compiling;

import java.io.IOException;

import javax.xml.transform.SourceLocator;

import org.knime.base.pmml.translation.PMMLTranslator;
import org.knime.base.pmml.translation.TerminatingMessageException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.ext.sun.nodes.script.compile.CompilationFailedException;
import org.knime.ext.sun.nodes.script.compile.JavaCodeCompiler;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.node.pmml.transformation.AbstractSparkTransformationPMMLApplyNodeModel;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObject;

import net.sf.saxon.s9api.MessageListener;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XdmNode;

/**
 * The PMML transformation node model.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkTransformationPMMLApplyNodeModel extends AbstractSparkTransformationPMMLApplyNodeModel
    implements MessageListener {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkTransformationPMMLApplyNodeModel.class);
    /**The name of the java package.*/
    private static final String PACKAGE_NAME = "";
    /**The name of the java class.*/
    private static final String MODEL_NAME = "MainModel";

    SparkTransformationPMMLApplyNodeModel() {
        super(new PortType[]{PMMLPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
//        final PMMLPortObjectSpec pmmlSpec = (PMMLPortObjectSpec) inSpecs[0];
//        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[1];
        return new PortObjectSpec[] {null};
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
     * @throws InvalidSettingsException
     * @throws SaxonApiException
     * @throws IOException
     */
    @Override
    public CompiledModelPortObject getCompiledPMMLModel(final ExecutionMonitor exec, final PortObject[] inObjects)
            throws Exception {
         final PMMLPortObject pmml = (PMMLPortObject)inObjects[0];
         String doc = pmml.getPMMLValue().toString();
         String code;
         try {
             code = PMMLTranslator.generateJava(doc, this, PACKAGE_NAME, MODEL_NAME);
         } catch (TerminatingMessageException tme) {
             throw new UnsupportedOperationException(tme.getMessage());
         }

         try {
             return new CompiledModelPortObject(code, PACKAGE_NAME, MODEL_NAME, JavaCodeCompiler.JavaVersion.JAVA_7);
         } catch (CompilationFailedException e) {
             throw new InvalidSettingsException("The compilation of the generated code failed.\n" + e.getMessage());
         }
    }
}
