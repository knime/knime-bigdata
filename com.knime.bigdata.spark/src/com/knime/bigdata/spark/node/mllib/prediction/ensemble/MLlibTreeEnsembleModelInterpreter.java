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
 *   Created on 27.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.ensemble;

import java.util.List;

import org.apache.spark.mllib.tree.model.TreeEnsembleModel;

import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.interpreter.HTMLModelInterpreter;

/**
 *
 * @author koetter
 * @param <M> {@link TreeEnsembleModel} implementation
 */
public abstract class MLlibTreeEnsembleModelInterpreter<M extends TreeEnsembleModel> extends
    HTMLModelInterpreter<SparkModel<M>> {

    private static final long serialVersionUID = 1L;

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final SparkModel<M> model) {
        final M treeModel = model.getModel();
        return "Number of trees: " + treeModel.numTrees() + " Total number of nodes: " + treeModel.totalNumNodes();
    }

    @Override
    protected String getHTMLDescription(final SparkModel<M> model) {
        final M treeModel = model.getModel();
        final StringBuilder buf = new StringBuilder();
        buf.append("Number of trees: " + treeModel.numTrees() + " Total number of nodes: " + treeModel.totalNumNodes());
        buf.append("<br>");
        buf.append(" Features: ");
        final List<String> colNames = model.getLearningColumnNames();
        int idx = 0;
        for (final String colName : colNames) {
            if (idx > 0) {
                buf.append(", ");
            }
            buf.append(idx++ + "=" + colName);
        }
        buf.append("<br>");
        String debugString = treeModel.toDebugString();
        //remove first line
        debugString = debugString.replaceFirst(".*\n", "");
        debugString = debugString.replaceAll("\\n", "<br>");
        debugString = debugString.replaceAll("<=", "&le");
        debugString = debugString.replaceAll(">=", "&ge");
        debugString = debugString.replaceAll("\\s", "&nbsp;");
        buf.append(debugString);
        return buf.toString();
    }

}