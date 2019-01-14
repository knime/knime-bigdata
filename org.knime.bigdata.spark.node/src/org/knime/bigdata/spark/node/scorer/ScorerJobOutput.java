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
 */
package org.knime.bigdata.spark.node.scorer;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Scorer job output.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class ScorerJobOutput extends JobOutput {

    private static final String MISSING_VALUE_ROW_COUNT = "missingValueRowCount";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public ScorerJobOutput() {}

    /**
     * @param missingValueRowCount count of rows with missing values
     */
    public ScorerJobOutput(final long missingValueRowCount) {
        set(MISSING_VALUE_ROW_COUNT, missingValueRowCount);
    }

    /**
     * @return count of rows with missing values
     */
    public long getMissingValueRowCount() {
        return getLong(MISSING_VALUE_ROW_COUNT);
    }
}
