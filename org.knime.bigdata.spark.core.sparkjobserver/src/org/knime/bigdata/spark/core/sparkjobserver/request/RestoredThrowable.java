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
 *   Created on Apr 5, 2017 by bjoern
 */
package org.knime.bigdata.spark.core.sparkjobserver.request;

/**
 * Instances of this class hold information (class, message and stack trace) about an exception that has been restored
 * from a Spark Jobserver response (JSON).
 *
 * <p>
 * For logging purposes, invoking {@link #getMessage()} on instance of this class will provide a message with format
 * "<exceptionClass>: <originalMessage>". If you want to only get the original message, please use
 * {@link #getOriginalMessage()}.
 * </p>
 *
 * @author Bjoern Lohrmann, KNIME.com GmbH
 */
public class RestoredThrowable extends Throwable {

    private static final long serialVersionUID = 1L;

    private final String m_originalMessage;

    /**
     * Creates a new instance.
     *
     * @param exceptionClass Name of the exception class that was thrown inside Spark Context. Must not be null.
     * @param message Message of the exception that was thrown inside Spark Context. Must not be null.
     */
    public RestoredThrowable(final String exceptionClass, final String message) {
        super(String.format("%s: %s", exceptionClass, message));
        m_originalMessage = message;
    }

    /**
     *
     * @return the message of the original exception.
     */
    public String getOriginalMessage() {
        return m_originalMessage;
    }
}
