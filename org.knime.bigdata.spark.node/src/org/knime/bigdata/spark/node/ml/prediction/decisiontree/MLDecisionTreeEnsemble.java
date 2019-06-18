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
 *   Created on Jun 17, 2019 by bjoern
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class MLDecisionTreeEnsemble {

    private final List<MLDecisionTree> m_trees;

    /**
     *
     * @param trees The trees in this ensemble.
     */
    public MLDecisionTreeEnsemble(final List<MLDecisionTree> trees) {
        super();
        m_trees = trees;
    }

    public void write(final DataOutputStream out) throws IOException {
        out.writeInt(m_trees.size());

        int[] jumpIndices = new int[m_trees.size()];

        int i = 0;
        for(MLDecisionTree tree : m_trees) {
            jumpIndices[i] = out.size();
            tree.write(out);
            i++;
        }

        // the jump indices are for a future optimization where
        // we read each tree individually on demand.
        for (i = 0; i < jumpIndices.length; i++) {
            out.writeInt(jumpIndices[i]);
        }
    }

    public static MLDecisionTreeEnsemble read(final DataInputStream in) throws IOException {
        final int treesToRead = in.readInt();
        final List<MLDecisionTree> trees = new ArrayList<>(treesToRead);

        for (int i = 0; i < treesToRead; i++) {
            trees.add(MLDecisionTree.read(in));
        }

        return new MLDecisionTreeEnsemble(trees);
    }

    public static MLDecisionTree readTreeAt(final RandomAccessFile file, final int treeIndex) throws IOException {
        file.seek(0);
        final int noOfTrees = file.readInt();

        final long jumpIndicesStartIndex = file.length() - noOfTrees * 4;
        file.seek(jumpIndicesStartIndex);

        final int[] jumpIndices = new int[noOfTrees];
        for(int i=0; i< noOfTrees; i++) {
            jumpIndices[i] = file.readInt();
        }

        file.seek(jumpIndices[treeIndex]);
        return MLDecisionTree.read(file);
    }
}
