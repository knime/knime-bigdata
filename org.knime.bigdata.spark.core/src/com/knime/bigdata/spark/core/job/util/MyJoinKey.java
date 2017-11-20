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
 *   Created on 12.08.2015 by dwk
 */
package com.knime.bigdata.spark.core.job.util;

import java.io.Serializable;
import java.util.Arrays;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * serializable key container for Join operations
 *
 * (two arrays not NOT equal when directly compared via 'equals' which is why we need this class so that we can override equals
 * and compare the elements of the arrays instead)
 * @author dwk
 */
@SparkClass
public class MyJoinKey implements Serializable {

    private static final long serialVersionUID = 1L;

    final Object[] m_values;

    /**
     * @param aValues
     */
    public MyJoinKey(final Object[] aValues) {
        m_values = aValues;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(m_values);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MyJoinKey other = (MyJoinKey)obj;
        if (!Arrays.equals(m_values, other.m_values)) {
            return false;
        }
        return true;
    }

}
