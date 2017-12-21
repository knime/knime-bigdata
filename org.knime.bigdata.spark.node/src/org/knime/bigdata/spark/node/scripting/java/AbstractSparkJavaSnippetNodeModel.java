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
package org.knime.bigdata.spark.node.scripting.java;

import org.knime.bigdata.spark.node.scripting.java.util.helper.AbstractJavaSnippetHelperRegistry;
import org.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelper.SnippetType;
import org.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelperRegistry;
import org.knime.core.node.port.PortType;

/**
 * RDD specific java snippet model.
 *
 * @author Tobias Koetter, KNIME GmbH
 */
public abstract class AbstractSparkJavaSnippetNodeModel extends AbstractSparkJavaSnippetBaseNodeModel {

    /** Unique job id */
    public static final String JOB_ID = "JavaSnippetJob";

    /**
     * Default constructor
     * @param inPortTypes
     * @param outPortTypes
     * @param snippetType Type of the snippet node (e.g. source, sink, ...)
     * @param isDeprecatedNode Indicates whether this node model instance belongs to one of the deprecated Java snippet
     *            nodes.
     */
    protected AbstractSparkJavaSnippetNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes,
            final SnippetType snippetType, final boolean isDeprecatedNode) {
        super(inPortTypes, outPortTypes, snippetType, isDeprecatedNode);
    }

    /** @return snippet helper registry */
    @Override
    protected AbstractJavaSnippetHelperRegistry getHelperRegistry() {
        return JavaSnippetHelperRegistry.getInstance();
    }
}