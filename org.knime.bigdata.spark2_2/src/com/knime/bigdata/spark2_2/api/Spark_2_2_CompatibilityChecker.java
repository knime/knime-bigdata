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
 *   Created on 27.04.2016 by koetter
 */
package com.knime.bigdata.spark2_2.api;

import com.knime.bigdata.spark.core.version.FixedVersionCompatibilityChecker;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark_2_2_CompatibilityChecker extends FixedVersionCompatibilityChecker {

    /**The only instance.*/
    public static final Spark_2_2_CompatibilityChecker INSTANCE = new Spark_2_2_CompatibilityChecker();

    private Spark_2_2_CompatibilityChecker() {
        super(SparkVersion.V_2_2);
    }
}
