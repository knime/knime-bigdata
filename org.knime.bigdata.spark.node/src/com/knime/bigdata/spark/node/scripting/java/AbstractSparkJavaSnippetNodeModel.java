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
 *   Created on 24.06.2015 by koetter
 */
package com.knime.bigdata.spark.node.scripting.java;

import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.node.scripting.java.util.helper.AbstractJavaSnippetHelperRegistry;
import com.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelper.SnippetType;
import com.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelperRegistry;

/**
 * RDD specific java snippet model
 * @author Tobias Koetter, KNIME.com
 */
public abstract class AbstractSparkJavaSnippetNodeModel extends AbstractSparkJavaSnippetBaseNodeModel {

    /** Unique job id */
    public static final String JOB_ID = "JavaSnippetJob";

    /**
     * Default constructor
     * @param inPortTypes
     * @param outPortTypes
     * @param snippetType Type of the snippet node (e.g. source, sink, ...)
     */
    protected AbstractSparkJavaSnippetNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes,
            final SnippetType snippetType) {
        super(inPortTypes, outPortTypes, snippetType);
    }

    /** @return snippet helper registry */
    @Override
    protected AbstractJavaSnippetHelperRegistry getHelperRegistry() {
        return JavaSnippetHelperRegistry.getInstance();
    }
}