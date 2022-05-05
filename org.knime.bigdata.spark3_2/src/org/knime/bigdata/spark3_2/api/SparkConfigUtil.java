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
package org.knime.bigdata.spark3_2.api;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Methods to access spark configuration properties.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public final class SparkConfigUtil {

    private SparkConfigUtil() {}

    /**
     * Verify if adaptive query execution is enabled in configuration of given spark context.
     *
     * @param sparkContext context to check
     * @return {@code true} if adaptive query execution is enabled
     */
    @SuppressWarnings("resource")
    public static boolean adaptiveExecutionEnabled(final SparkContext sparkContext) {
        return SparkSession.builder().sparkContext(sparkContext).getOrCreate().sqlContext().conf()
            .adaptiveExecutionEnabled();
    }

}
