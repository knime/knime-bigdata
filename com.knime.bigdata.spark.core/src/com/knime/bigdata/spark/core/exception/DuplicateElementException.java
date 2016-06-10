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
 *   Created on May 12, 2016 by oole
 */
package com.knime.bigdata.spark.core.exception;

/**
 *
 * @author oole
 */
public class DuplicateElementException extends Exception {

    private static final long serialVersionUID = 1L;
    private Object m_id;

    /**
     * Constructs a new exception with the specified detail message.  The
     * cause is not initialized, and may subsequently be initialized by
     * a call to {@link #initCause}.
     * @param id the id of the problematic element
     *
     * @param   message   the detail message. The detail message is saved for
     *          later retrieval by the {@link #getMessage()} method.
     */
    public DuplicateElementException(final Object id, final String message) {
        super(message);
        m_id = id;
    }

    /**
     * @return the id of the problematic element
     */
    public Object getId() {
        return m_id;
    }
}
