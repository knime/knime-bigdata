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
package org.knime.bigdata.spark1_6.api;

import org.knime.bigdata.spark.core.version.FixedVersionCompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class AllSpark_1_6_CompatibilityChecker extends FixedVersionCompatibilityChecker {

    /**The only instance.*/
    public static final AllSpark_1_6_CompatibilityChecker INSTANCE = new AllSpark_1_6_CompatibilityChecker();

    private AllSpark_1_6_CompatibilityChecker() {
        super(SparkVersion.V_1_6, SparkVersion.V_1_6_CDH_5_9);
    }
}
