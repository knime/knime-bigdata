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
 *   Created on Apr 11, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.types.intermediate;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author bjoern
 */
@SparkClass
public class IntermediateArrayDataType extends IntermediateDataType {

    private static final long serialVersionUID = 1L;

    private IntermediateDataType m_baseType;

    /**
     * @param baseType
     */
    public IntermediateArrayDataType(final IntermediateDataType baseType) {
        super("array");
        m_baseType = baseType;
    }

    /**
     * @return the baseType
     */
    public IntermediateDataType getBaseType() {
        return m_baseType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((m_baseType == null) ? 0 : m_baseType.hashCode());
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
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IntermediateArrayDataType other = (IntermediateArrayDataType)obj;
        if (m_baseType == null) {
            if (other.m_baseType != null) {
                return false;
            }
        } else if (!m_baseType.equals(other.m_baseType)) {
            return false;
        }
        return true;
    }
}
