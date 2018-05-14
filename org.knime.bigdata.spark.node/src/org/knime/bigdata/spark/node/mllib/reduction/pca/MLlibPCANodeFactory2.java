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
 */
package org.knime.bigdata.spark.node.mllib.reduction.pca;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;

/**
 * Spark PCA node
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class MLlibPCANodeFactory2 extends DefaultSparkNodeFactory<MLlibPCANodeModel> {

    /** Default Constructor */
    public MLlibPCANodeFactory2() {
        super("mining/reduction");
    }

    @Override
    public MLlibPCANodeModel createNodeModel() {
        return new MLlibPCANodeModel(false);
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new MLlibPCANodeDialog(false);
    }
}
