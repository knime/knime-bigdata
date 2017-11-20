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
 *   Created on Apr 14, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.exception;

/**
 *
 *
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class MissingSparkModelHelperException extends Exception {

    private static final long serialVersionUID = 1L;

    public MissingSparkModelHelperException(final String message) {
        super(message);
    }
}
