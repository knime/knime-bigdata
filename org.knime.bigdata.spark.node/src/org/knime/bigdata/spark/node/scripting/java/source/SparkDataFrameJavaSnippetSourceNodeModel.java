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
 *   Created on 29.05.2015 by koetter
 */
package org.knime.bigdata.spark.node.scripting.java.source;

import org.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.node.scripting.java.AbstractSparkDataFrameJavaSnippetNodeModel;
import org.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelper.SnippetType;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkDataFrameJavaSnippetSourceNodeModel extends AbstractSparkDataFrameJavaSnippetNodeModel {

    /**
     * Default constructor.
     * @param optionalSparkPort true if input spark context port is optional
     */
    public SparkDataFrameJavaSnippetSourceNodeModel(final boolean optionalSparkPort) {
        super(SparkSourceNodeModel.addContextPort(null, optionalSparkPort),
            new PortType[]{SparkDataPortObject.TYPE}, SnippetType.SOURCE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        super.configureInternal(inSpecs);
        return new PortObjectSpec[] {null};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        return super.executeSnippetJob(inData, true, exec, JOB_ID);
    }
}
