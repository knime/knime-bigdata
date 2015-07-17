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
 *   Created on 17.07.2015 by Dietrich
 */
package com.knime.bigdata.spark.jobserver.server;

import java.io.Serializable;

/**
 *
 * @author dwk
 */
public class MyRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * index of the source column (or -1 for global mappings)
     */
    final public int m_nominalColumnIndex;

    /**
     * the source string value
     */
    final public String m_nominalValue;

    /**
     * the mapped value
     */
    final public int m_numberValue;

    MyRecord(final int col, final String nomVal, final int numVal) {
        m_nominalColumnIndex = col;
        m_nominalValue = nomVal;
        m_numberValue = numVal;
    }
}
