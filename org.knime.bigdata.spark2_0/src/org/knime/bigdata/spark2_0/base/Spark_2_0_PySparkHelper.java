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
 *   Created on 28.08.2018 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark2_0.base;

import org.knime.bigdata.spark.node.scripting.python.util.DefaultPySparkHelper;
import org.knime.bigdata.spark2_0.api.Spark_2_0_CompatibilityChecker;
import org.knime.bigdata.spark2_0.jobs.scripting.python.PySparkDataExchanger;

/**
 * The PySpark helper class for Spark 2.0
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class Spark_2_0_PySparkHelper extends DefaultPySparkHelper {

    /**
     * Constructs a PySparkHelper for Spark 2.0
     */
    public Spark_2_0_PySparkHelper() {
        super(Spark_2_0_CompatibilityChecker.INSTANCE, PySparkDataExchanger.class.getCanonicalName());
    }
}
