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
 *   Created on 08.02.2016 by koetter
 */
package com.knime.bigdata.spark.core.port.data;

import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class FetchRowsJobInput extends JobInput {

    private static final String KEY_NUMBER_OF_ROWS = "numRows";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public FetchRowsJobInput() {
    }

    public int getNumberOfRows() {
        return getInteger(KEY_NUMBER_OF_ROWS);
    }

    public static FetchRowsJobInput create(final int numRows, final String namedInputObjectId,
        final IntermediateSpec namedInputObjectSpec) {

        FetchRowsJobInput input = new FetchRowsJobInput();
        input.set(KEY_NUMBER_OF_ROWS, numRows);
        input.addNamedInputObject(namedInputObjectId);
        input.withSpec(namedInputObjectId, namedInputObjectSpec);
        return input;
    }
}
