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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark.node.pmml.transformation;

import java.util.List;
import java.util.Map;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.node.pmml.PMMLAssignJobInput;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class PMMLTransformationJobInput extends PMMLAssignJobInput {

    private static final String REPLACE = "REPLACE";
    private static final String ADD_COLS = "additionalCols";
    private static final String SKIP_COLS = "skippedCols";
    private static final String REPLACE_COLS = "replaceCols";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public PMMLTransformationJobInput() {}

    /**
     * @param inputNamedObject
     * @param colIdxs
     * @param mainClass
     * @param outputNamedObject
     * @param outputSpec
     * @param addCols
     * @param replace
     * @param skipCols
     * @param replaceCols
     */
    public PMMLTransformationJobInput(final String inputNamedObject, final Integer[] colIdxs, final String mainClass,
            final String outputNamedObject, final IntermediateSpec outputSpec,
            final List<Integer> addCols, final boolean replace, final List<Integer> skipCols, final Map<Integer, Integer> replaceCols) {

        super(inputNamedObject, colIdxs, mainClass, outputNamedObject, outputSpec);
        set(REPLACE, replace);
        set(ADD_COLS, addCols);
        set(SKIP_COLS, skipCols);
        set(REPLACE_COLS, replaceCols);
    }

    /**
     * @return whether to replace or not
     */
    public boolean replace() {
        return get(REPLACE);
    }

    /**
     * @return a list of additional column indices
     */
    public List<Integer> getAdditionalColIdxs() {
        return get(ADD_COLS);
    }

    /**
     * @return a list of columnd indices to skip
     */
    public List<Integer> getSkippedColIdxs() {
        return get(SKIP_COLS);
    }

    /**
     * @return map of input column indices (key) to replace with PMML result column indices (value)
     */
    public Map<Integer, Integer> getReplaceColIdxs() {
        return get(REPLACE_COLS);
    }
}
