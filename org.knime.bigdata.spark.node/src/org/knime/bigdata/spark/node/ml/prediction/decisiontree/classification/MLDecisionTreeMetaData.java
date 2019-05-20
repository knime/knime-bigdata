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
 *   Created on May 31, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree.classification;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLDecisionTreeMetaData extends MLMetaData {

    private static final String KEY_NUM_NODES = "numNodes";

    private static final String KEY_DEPTH = "depth";

    /**
     * Constructor for (de)serialization.
     */
    public MLDecisionTreeMetaData() {
    }

    /**
     *
     * @param numNodes Number of nodes in tree.
     * @param depth Tree depth.
     */
    public MLDecisionTreeMetaData(final int numNodes, final int depth) {
        set(KEY_NUM_NODES, numNodes);
        set(KEY_DEPTH, depth);
    }

    /**
     *
     * @return number of nodes in tree.
     */
    public int getNumberOfTreeNodes() {
        return getInteger(KEY_NUM_NODES);
    }

    /**
     *
     * @return depth of tree.
     */
    public int getTreeDepth() {
        return getInteger(KEY_DEPTH);
    }
}
