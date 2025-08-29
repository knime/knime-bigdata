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
 *   Created on 12.02.2015 by koetter
 */
package org.knime.bigdata.spark.node.pmml.predictor.compiling;

import javax.xml.transform.SourceLocator;

import org.knime.base.pmml.translation.PMMLTranslator;
import org.knime.base.pmml.translation.TerminatingMessageException;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.node.pmml.predictor.AbstractSparkPMMLPredictorNodeModel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.ext.sun.nodes.script.compile.CompilationFailedException;
import org.knime.ext.sun.nodes.script.compile.JavaCodeCompiler;

import com.knime.pmml.compilation.java.compile.CompiledModelPortObject;

import net.sf.saxon.s9api.MessageListener;
import net.sf.saxon.s9api.XdmNode;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkPMMLCompilingPredictorNodeModel extends AbstractSparkPMMLPredictorNodeModel
implements MessageListener {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkPMMLCompilingPredictorNodeModel.class);
    /**The name of the java package.*/
    private static final String PACKAGE_NAME = "";
    /**The name of the java class.*/
    private static final String MODEL_NAME = "MainModel";

    /**
     *
     */
    public SparkPMMLCompilingPredictorNodeModel() {
        super(new PortType[]{PMMLPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec getResultSpec(final PortObjectSpec pmmlSpec, final SparkDataPortObjectSpec sparkSpec,
        final String predColname, final boolean outputProbabilities, final String suffix) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CompiledModelPortObject getCompiledModel(final PortObject inObject) throws Exception {
        final PMMLPortObject pmml = (PMMLPortObject)inObject;
        final String doc = pmml.getPMMLValue().toString();
        try {
            final String code = PMMLTranslator.generateJava(doc, this, PACKAGE_NAME, MODEL_NAME);
            CompiledModelPortObject cm = new CompiledModelPortObject(code, PACKAGE_NAME, MODEL_NAME,
                JavaCodeCompiler.JavaVersion.JAVA_8);
            return cm;
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
}
