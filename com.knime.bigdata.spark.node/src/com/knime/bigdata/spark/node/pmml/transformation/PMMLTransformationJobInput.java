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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark.node.pmml.transformation;

import java.util.List;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.pmml.PMMLAssignJobInput;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class PMMLTransformationJobInput extends PMMLAssignJobInput {

    private static final String REPLACE = "REPLACE";
    private static final String ADD_COLS = "additionalCols";
    private static final String SKIP_COLS = "skippedCols";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public PMMLTransformationJobInput() {}

    /**
     * @param inputNamedObject
     * @param colIdxs
     * @param mainClass
     * @param outputNamedObject
     * @param addCols
     * @param replace
     * @param skipCols
     */
    public PMMLTransformationJobInput(final String inputNamedObject, final Integer[] colIdxs, final String mainClass,
        final String outputNamedObject, final List<Integer> addCols, final boolean replace,
        final List<Integer> skipCols) {
        super(inputNamedObject, colIdxs, mainClass, outputNamedObject);
        set(REPLACE, replace);
        set(ADD_COLS, addCols);
        set(SKIP_COLS, skipCols);
    }

    /**
     * @return
     */
    public boolean replace() {
        return get(REPLACE);
    }

    /**
     * @return
     */
    public List<Integer> getAdditionalColIdxs() {
        return get(ADD_COLS);
    }

    /**
     * @return
     */
    public List<Integer> getSkippedColIdxs() {
        return get(SKIP_COLS);
    }
}
