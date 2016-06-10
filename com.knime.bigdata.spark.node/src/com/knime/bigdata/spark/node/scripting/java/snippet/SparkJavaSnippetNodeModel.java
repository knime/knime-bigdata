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
 *   Created on 29.05.2015 by koetter
 */
package com.knime.bigdata.spark.node.scripting.java.snippet;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.node.scripting.java.AbstractSparkJavaSnippetNodeModel;
import com.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelper.SnippetType;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkJavaSnippetNodeModel extends AbstractSparkJavaSnippetNodeModel {

    /** Constructor.*/
    public SparkJavaSnippetNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE, SparkDataPortObject.TYPE_OPTIONAL},
            new PortType[]{SparkDataPortObject.TYPE}, SnippetType.INNER);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || !(inSpecs[0] instanceof SparkDataPortObjectSpec)) {
            throw new InvalidSettingsException("Please connect the first inport of the node with an RDD outport");
        }
        super.configureInternal(inSpecs);
        return new PortObjectSpec[] {null};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        if (inData == null || inData.length < 1 || !(inData[0] instanceof SparkDataPortObject)) {
            throw new InvalidSettingsException("Please connect the first inport of the node with an RDD outport");
        }
        return super.executeSnippetJob(inData, true, exec);
    }
}
