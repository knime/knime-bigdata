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
package com.knime.bigdata.spark.node.scripting.java.snippet;

import org.knime.core.node.NodeDialogPane;

import com.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import com.knime.bigdata.spark.node.scripting.java.SparkJavaSnippetNodeDialog;
import com.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelper.SnippetType;
import com.knime.bigdata.spark.node.scripting.java.util.helper.JavaSnippetHelperRegistry;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkJavaSnippetNodeFactory extends DefaultSparkNodeFactory<SparkJavaSnippetNodeModel> {

    /**
     * Constructor.
     */
    public SparkJavaSnippetNodeFactory() {
        super("misc/java");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkJavaSnippetNodeModel createNodeModel() {
        return new SparkJavaSnippetNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new SparkJavaSnippetNodeDialog(this.getClass(), SnippetType.INNER, JavaSnippetHelperRegistry.getInstance());
    }
}
