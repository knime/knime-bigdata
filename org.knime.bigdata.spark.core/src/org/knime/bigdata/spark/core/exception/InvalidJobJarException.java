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
package org.knime.bigdata.spark.core.exception;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * This exception indicates a different job jar version on the Spark context and KNIME instance side.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class InvalidJobJarException extends KNIMESparkException {

    private static final long serialVersionUID = 1L;

    /**
     * Default constructor.
     * @param message Human readable error message.
     */
    public InvalidJobJarException(final String message) {
        super(message);
    }
}
