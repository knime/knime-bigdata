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
package org.knime.bigdata.spark.node.pmml;

import java.util.Arrays;

import org.knime.bigdata.spark.core.job.ColumnsJobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public abstract class PMMLAssignJobInput extends ColumnsJobInput {

    private static final String CLASS = "mainClass";
    private static final String LONG_FIELDS = "longFields";

    /**
     * Paramless constuctor for automatic deserialization.
     */
    public PMMLAssignJobInput() {}

    /**
     * @param inputID
     * @param inputSpec
     * @param colIdxs
     * @param mainClass
     * @param outputID
     * @param outputSpec
     */
    public PMMLAssignJobInput(final String inputID, final IntermediateSpec inputSpec, final Integer[] colIdxs,
            final String mainClass, final String outputID, final IntermediateSpec outputSpec) {

        super(inputID, outputID, colIdxs);
        withSpec(outputID, outputSpec);
        set(CLASS, mainClass);
        set(LONG_FIELDS, getLongColumns(colIdxs, inputSpec));
    }

    /**
     * @return the name of the PMML main class
     */
    public String getMainClass() {
        return get(CLASS);
    }

    /** @return array with <code>true</code> if PMML input field is a long */
    public boolean[] getInputLongFields() {
        return get(LONG_FIELDS);
    }

    /**
     * Returns an array with <code>true</code> if input field of PMML method is a <code>long</code>.
     * @param inputColIdxs array with input indices used by PMML
     * @param inputSpec input spec
     * @return array with boolean flag (for each PMML input field) indicating if columns contains longs
     */
    private boolean[] getLongColumns(final Integer inputColIdxs[], final IntermediateSpec inputSpec) {
        final IntermediateField fields[] = inputSpec.getFields();
        final boolean longColumns[] = new boolean[inputColIdxs.length];
        Arrays.fill(longColumns, false);
        for (int i = 0; i < inputColIdxs.length; i++) {
            longColumns[i] =
                inputColIdxs[i] >= 0 && fields[inputColIdxs[i]].getType() == IntermediateDataTypes.LONG;
        }
        return longColumns;
    }
}
