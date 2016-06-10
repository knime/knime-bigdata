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
 *   Created on 28.01.2016 by koetter
 */
package com.knime.bigdata.spark.core.types.intermediate;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class IntermediateSpec implements Serializable, Iterable<IntermediateField> {

    private static final long serialVersionUID = 1L;

    private final IntermediateField[] m_fields;

    /**
     * @param fields {@link IntermediateField}
     *
     */
    public IntermediateSpec(final IntermediateField[] fields) {
        m_fields = fields;
    }

    /**
     * @return the fields
     */
    public IntermediateField[] getFields() {
        return m_fields;
    }

    /**
     * @return the number of fields in this spec.
     */
    public int getNoOfFields() {
        return m_fields.length;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) { return false; }
        if (obj == this) { return true; }
        if (obj.getClass() != getClass()) {
          return false;
        }

        // Why so complicated? Because we may not have Apache's commons.lang or a comparable
        // library on both the KNIME AP and the Spark side

        IntermediateSpec rhs = (IntermediateSpec) obj;
        if (m_fields.length != rhs.m_fields.length) {
            return false;
        }

        for (int i = 0; i < m_fields.length; i++) {
            if (!m_fields[i].equals(rhs.m_fields[i])) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        final int constant = 23;

        // Why so complicated? Because we may not have Apache's commons.lang or a comparable
        // library on both the KNIME AP and the Spark side

        int hashCode = 103;
        for (int i = 0; i < m_fields.length; i++) {
            hashCode = hashCode * constant + m_fields[i].hashCode();
        }

        return hashCode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<IntermediateField> iterator() {
        return Arrays.asList(m_fields).iterator();
    }
}
