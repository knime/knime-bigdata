/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
package com.knime.bigdata.spark.core.exception;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MissingJobException extends KNIMESparkException {

    private static final long serialVersionUID = 1L;

    public MissingJobException() {
        super("Could not find required Spark jobs. Possible reason: The extension that provides the KNIME Spark Executor jobs for your Spark version is not installed.");
    }
}
