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
package com.knime.bigdata.spark.node.scripting.java.source;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.jobserver.jobs.AbstractSparkJavaSnippetSource;
import com.knime.bigdata.spark.node.scripting.java.AbstractSparkJavaSnippetNodeModel;
import com.knime.bigdata.spark.node.scripting.java.util.SparkJavaSnippet;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkJavaSnippetSourceNodeModel extends AbstractSparkJavaSnippetNodeModel {
    static final String CLASS_NAME = "SparkJavaSnippetSource";
    static String METHOD_SIGNATURE = "public JavaRDD<Row> apply(final JavaSparkContext sc) "
        + "throws GenericKnimeSparkException";

    static SparkJavaSnippet createSnippet() {
        return new SparkJavaSnippet(CLASS_NAME, AbstractSparkJavaSnippetSource.class, METHOD_SIGNATURE);
    }

    /** Constructor.*/
    public SparkJavaSnippetSourceNodeModel() {
        super(new PortType[]{}, new PortType[]{SparkDataPortObject.TYPE}, createSnippet(),
            "return sc.<Row>emptyRDD();");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        PortObject[] result = super.executeInternal(inData, exec);
        if (result == null || result.length != 1 || !(result[0] instanceof SparkDataPortObject)) {
            throw new InvalidSettingsException("Snippet has to return a JavaRDD<Row> object");
        }
        return result;
    }
}
