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

import com.knime.bigdata.spark.jobserver.jobs.AbstractSparkJavaSnippet;
import com.knime.bigdata.spark.node.scripting.java.AbstractSparkJavaSnippetNodeModel;
import com.knime.bigdata.spark.node.scripting.java.util.SparkJavaSnippet;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkJavaSnippetNodeModel extends AbstractSparkJavaSnippetNodeModel {
    private static final String CLASS_NAME = "SparkJavaSnippet";
    private static String METHOD_SIGNATURE =
        "public JavaRDD<Row> apply(final JavaSparkContext sc, final JavaRDD<Row> rowRDD1, final JavaRDD<Row> rowRDD2)"
            + " throws GenericKnimeSparkException";

    static SparkJavaSnippet createSnippet() {
        return new SparkJavaSnippet(CLASS_NAME, AbstractSparkJavaSnippet.class, METHOD_SIGNATURE);
    }

    /** Constructor.*/
    public SparkJavaSnippetNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE, SparkDataPortObject.TYPE_OPTIONAL},
            new PortType[]{SparkDataPortObject.TYPE}, createSnippet(), "return rowRDD1;");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || !(inSpecs[0] instanceof SparkDataPortObjectSpec)) {
            throw new InvalidSettingsException("Please connect the first inport of the node with an RDD outport");
        }
        return super.configure(inSpecs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        if (inData == null || inData.length < 1 || !(inData[0] instanceof SparkDataPortObject)) {
            throw new InvalidSettingsException("Please connect the first inport of the node with an RDD outport");
        }
        PortObject[] result = super.executeInternal(inData, exec);
        if (result == null || result.length != 1 || !(result[0] instanceof SparkDataPortObject)) {
            throw new InvalidSettingsException("Snippet has to return a JavaRDD<Row> object");
        }
        return result;
    }
}
