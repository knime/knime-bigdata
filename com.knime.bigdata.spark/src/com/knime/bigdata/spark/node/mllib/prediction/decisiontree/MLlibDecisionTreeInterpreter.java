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
 *   Created on 21.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import java.util.List;

import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.interpreter.HTMLModelInterpreter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibDecisionTreeInterpreter extends HTMLModelInterpreter<SparkModel<DecisionTreeModel>> {

    private static final long serialVersionUID = 1L;

    private static volatile MLlibDecisionTreeInterpreter instance;

    private MLlibDecisionTreeInterpreter() {
        //avoid object creation
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static MLlibDecisionTreeInterpreter getInstance() {
        if (instance == null) {
            synchronized (MLlibDecisionTreeInterpreter.class) {
                if (instance == null) {
                    instance = new MLlibDecisionTreeInterpreter();
                }
            }
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getModelName() {
        return "DecisionTree";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary(final SparkModel<DecisionTreeModel> model) {
        final DecisionTreeModel treeModel = model.getModel();
        return "Tree depth: " + treeModel.depth() + " Number of nodes: " + treeModel.numNodes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHTMLDescription(final SparkModel<DecisionTreeModel> model) {
        final DecisionTreeModel treeModel = model.getModel();
        final StringBuilder buf = new StringBuilder();
        buf.append("Tree depth: " + treeModel.depth()
                + " Number of nodes: " + treeModel.numNodes());
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
