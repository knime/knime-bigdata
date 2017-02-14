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
package com.knime.bigdata.spark.node.pmml;

import com.knime.bigdata.spark.core.job.ColumnsJobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public abstract class PMMLAssignJobInput extends ColumnsJobInput {

    private static final String CLASS = "mainClass";

    /**
     * Paramless constuctor for automatic deserialization.
     */
    public PMMLAssignJobInput() {}

    /**
     * @param inputID
     * @param colIdxs
     * @param mainClass
     * @param outputID
     * @param outputSpec
     */
    public PMMLAssignJobInput(final String inputID, final Integer[] colIdxs, final String mainClass,
            final String outputID, final IntermediateSpec outputSpec) {

        super(inputID, outputID, colIdxs);
        withSpec(outputID, outputSpec);
        set(CLASS, mainClass);
    }

    /**
     * @return the name of the PMML main class
     */
    public String getMainClass() {
        return get(CLASS);
    }

}
