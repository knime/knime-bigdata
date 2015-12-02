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
 *   Created on 12.08.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.server;

import java.io.Serializable;
import java.util.Arrays;

/**
 *
 * serializable key container for sort operations by multiple values
 *
 * @author dwk
 */
public class MultiValueSortKey implements Serializable, Comparable<MultiValueSortKey> {

    private static final long serialVersionUID = 1L;

    final Object[] m_values;

    final Boolean[] m_isSortAscending;

    private Boolean m_aMissingToEnd;

    /**
     * @param aValues
     * @param aIsSortAscending
     * @param aMissingToEnd
     */
    public MultiValueSortKey(final Object[] aValues, final Boolean[] aIsSortAscending, final Boolean aMissingToEnd) {
        m_values = aValues;
        m_isSortAscending = aIsSortAscending;
        m_aMissingToEnd = aMissingToEnd;
    }

    /**
     *
     * @param aValues
     * @param aIsSortAscending
     */
    public MultiValueSortKey(final Object[] aValues, final Boolean[] aIsSortAscending) {
        this(aValues, aIsSortAscending, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(m_isSortAscending);
        result = prime * result + Arrays.hashCode(m_values);
        result = prime * result + m_aMissingToEnd.hashCode();
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
        MultiValueSortKey other = (MultiValueSortKey)obj;
        if (!Arrays.equals(m_isSortAscending, other.m_isSortAscending)) {
            return false;
        }
        if (!Arrays.equals(m_values, other.m_values)) {
            return false;
        }
        if (m_aMissingToEnd != other.m_aMissingToEnd) {
            return false;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(final MultiValueSortKey aOther) {
        return compareTo(aOther, 0);
    }

    private int compareTo(final MultiValueSortKey aOther, final int aLevel) {
        if (aLevel >= m_values.length) {
            return 0;
        }
        if (m_values[aLevel] == null || aOther.m_values[aLevel] == null) {
            if (m_values[aLevel] == null && aOther.m_values[aLevel] == null) {
                return compareTo(aOther, aLevel + 1);
            }
            int m = -1;
            if (m_values[aLevel] == null) {
                m = 1;
            }
            if (m_aMissingToEnd) {
                if (m > 0) {
                    //this object is null
                    return +1;
                }
                //the aOther object is null
                return -1;
            }
            if (m_isSortAscending[aLevel]) {
                return -1 * m;
            }
            return m;
        }
        if (m_values[aLevel].equals(aOther.m_values[aLevel])) {
            return compareTo(aOther, aLevel + 1);
        }
        Object o1 = m_values[aLevel];
        Object o2 = aOther.m_values[aLevel];
        if (o1 instanceof Number && o2 instanceof Number) {
            final boolean isSmaller = ((Number)o1).doubleValue() < ((Number)o2).doubleValue();
            if (isSmaller == m_isSortAscending[aLevel]) {
                return -1;
            }
            return 1;
        } else {
            //compare everything else as a String
            int c = o1.toString().compareTo(o2.toString());
            if (!m_isSortAscending[aLevel]) {
                return -1 * c;
            }
            return c;
        }
    }

}
