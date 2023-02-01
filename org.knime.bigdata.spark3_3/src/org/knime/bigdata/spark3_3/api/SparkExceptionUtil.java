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
 */
package org.knime.bigdata.spark3_3.api;

import org.apache.spark.SparkException;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Utilities to validate {@link SparkException}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public final class SparkExceptionUtil {

    private SparkExceptionUtil() {}

    /**
     * Validate if given exception contains a missing value exception from {@link StringIndexer} or
     * {@link VectorAssembler} running with handle invalid as error parameter.
     *
     * @param e exception to validate
     * @return {@code true} if exception contains a missing value message
     */
    public static boolean isMissingValueException(final Throwable e) {
        if (e instanceof SparkException) {
            final String msg = e.getMessage();
            return msg.contains("StringIndexer encountered NULL value") // StringIndexer
                || msg.contains("Encountered NaN while assembling a row with handleInvalid = \"error\".") // VectorAssembler
                || msg.contains("Encountered null while assembling a row with handleInvalid = \"error\".") // VectorAssembler
                || isMissingValueException(e.getCause());
        } else {
            return false;
        }
    }
}
