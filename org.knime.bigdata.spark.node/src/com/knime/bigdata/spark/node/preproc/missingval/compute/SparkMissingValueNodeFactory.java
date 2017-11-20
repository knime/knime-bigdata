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
 */
package com.knime.bigdata.spark.node.preproc.missingval.compute;

import java.io.IOException;

import org.knime.base.node.preproc.pmml.missingval.utils.MissingValueNodeDescriptionHelper;
import org.knime.core.node.NodeDescription;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;
import org.xml.sax.SAXException;

import com.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import com.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandlerFactoryManager;

/**
 * Missing value spark node factory.
 * 
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkMissingValueNodeFactory extends DefaultSparkNodeFactory<SparkMissingValueNodeModel> {

    /** Missing value spark node factory */
    public SparkMissingValueNodeFactory() {
        super("column/transform");
    }

    @Override
    public SparkMissingValueNodeModel createNodeModel() {
        return new SparkMissingValueNodeModel();
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<SparkMissingValueNodeModel> createNodeView(final int viewIndex,
        final SparkMissingValueNodeModel nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new SparkMissingValueNodeDialog();
    }

    @Override
    protected NodeDescription createNodeDescription() {
        NodeDescription createNodeDescription = super.createNodeDescription();
        try {
            SparkMissingValueHandlerFactoryManager manager = SparkMissingValueHandlerFactoryManager.getInstance();
            return MissingValueNodeDescriptionHelper.createNodeDescription(createNodeDescription, manager);
        } catch (SAXException | IOException e) {
            return createNodeDescription;
        }
    }
}
