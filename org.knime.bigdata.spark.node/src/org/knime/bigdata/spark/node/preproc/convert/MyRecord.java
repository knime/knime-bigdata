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
 *   Created on 17.07.2015 by dwk
 */
package org.knime.bigdata.spark.node.preproc.convert;

import java.io.Serializable;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author dwk
 */
@SparkClass
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
     * the mapped value (for column mapping actual value, for binary mapping new column index, but counting
     * from 0 for each respective column)
     */
    final public int m_numberValue;

    /**
     * @param col
     * @param nomVal
     * @param numVal
     */
    public MyRecord(final int col, final String nomVal, final int numVal) {
        m_nominalColumnIndex = col;
        m_nominalValue = nomVal;
        m_numberValue = numVal;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(m_nominalColumnIndex);
        sb.append("|");
        sb.append(m_nominalValue);
        sb.append("|");
        sb.append(m_numberValue);
        return sb.toString();
    }
}
