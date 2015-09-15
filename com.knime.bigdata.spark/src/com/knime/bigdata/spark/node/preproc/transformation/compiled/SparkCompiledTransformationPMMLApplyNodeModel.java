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
package com.knime.bigdata.spark.node.preproc.transformation.compiled;

import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.node.preproc.transformation.AbstractSparkTransformationPMMLApplyNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.pmml.compilation.java.compile.CompiledModelPortObject;

/**
 * The PMML transformation node model.
 *
 * @author koetter
 */
public class SparkCompiledTransformationPMMLApplyNodeModel extends AbstractSparkTransformationPMMLApplyNodeModel {


    SparkCompiledTransformationPMMLApplyNodeModel() {
        super(new PortType[]{CompiledModelPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompiledModelPortObject getCompiledPMMLModel(final ExecutionMonitor exec, final PortObject[] inObjects) {
        return (CompiledModelPortObject)inObjects[0];
    }
}
