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
package org.knime.bigdata.spark2_4.api;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * General spark methods.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public final class SparkUtil {

    private SparkUtil() {}

    /**
     * Surround given column name with backticks to support special characters like dots.
     *
     * @param name column name to escape
     * @return name surrounded by backticks
     */
    public static String quoteColumnName(final String name) {
        return String.format("`%s`", name);
    }
}
