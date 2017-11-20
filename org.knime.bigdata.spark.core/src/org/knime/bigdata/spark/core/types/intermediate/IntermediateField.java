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
 *   Created on 28.01.2016 by koetter
 */
package org.knime.bigdata.spark.core.types.intermediate;

import java.io.Serializable;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class IntermediateField implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String m_name;
    private final IntermediateDataType m_type;
    private final boolean m_nullable;


    /**
     * @param name
     * @param type
     */
    public IntermediateField(final String name, final IntermediateDataType type) {
        this(name, type, true);
    }

    /**
     * @param name
     * @param type
     * @param nullable
     */
    public IntermediateField(final String name, final IntermediateDataType type, final boolean nullable) {
        m_name = name;
        m_type = type;
        m_nullable = nullable;
    }

    /**
     * @return the name
     */
    public String getName() {
        return m_name;
    }

    /**
     * @return the type
     */
    public IntermediateDataType getType() {
        return m_type;
    }

    /**
     * @return the nullable
     */
    public boolean isNullable() {
        return m_nullable;
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

        IntermediateField rhs = (IntermediateField) obj;
        return m_name.equals(rhs.m_name)
              && m_type.equals(rhs.m_type)
              && m_nullable == rhs.m_nullable;
    }

    @Override
    public int hashCode() {
        final int constant = 79;

        // Why so complicated? Because we may not have Apache's commons.lang or a comparable
        // library on both the KNIME AP and the Spark side

        int hashCode = 23 * constant + m_name.hashCode();
        hashCode = hashCode * constant + m_type.hashCode();
        hashCode = hashCode * constant + (m_nullable ? 1 : 0);

        return hashCode;
    }
}
