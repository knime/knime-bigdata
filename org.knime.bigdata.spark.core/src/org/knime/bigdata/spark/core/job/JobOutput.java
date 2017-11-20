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
 *   Created on 29.01.2016 by koetter
 */
package org.knime.bigdata.spark.core.job;

/**
 *
 * @author Tobias Koetter, Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public abstract class JobOutput extends JobData {

    private final static String KEY_PREFIX = "out";

    /**
     * Creates empty job output. Only for use by subclasses or factory methods.
     */
    protected JobOutput() {
        super(KEY_PREFIX);
    }
}
