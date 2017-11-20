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
package com.knime.bigdata.spark.core.types.intermediate;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author bjoern
 */
@SparkClass
public class IntermediateMapDataType extends IntermediateDataType {

    private static final long serialVersionUID = 1L;

    private final IntermediateDataType m_keyType;

    private final IntermediateDataType m_valueType;

    /**
     * @param keyType
     * @param valueType
     */
    protected IntermediateMapDataType(final IntermediateDataType keyType, final IntermediateDataType valueType) {
        super("map");
        m_keyType = keyType;
        m_valueType = valueType;
    }

    /**
     * @return the keyType
     */
    public IntermediateDataType getKeyType() {
        return m_keyType;
    }

    /**
     * @return the valueType
     */
    public IntermediateDataType getValueType() {
        return m_valueType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((m_keyType == null) ? 0 : m_keyType.hashCode());
        result = prime * result + ((m_valueType == null) ? 0 : m_valueType.hashCode());
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
        IntermediateMapDataType other = (IntermediateMapDataType)obj;
        if (m_keyType == null) {
            if (other.m_keyType != null) {
                return false;
            }
        } else if (!m_keyType.equals(other.m_keyType)) {
            return false;
        }
        if (m_valueType == null) {
            if (other.m_valueType != null) {
                return false;
            }
        } else if (!m_valueType.equals(other.m_valueType)) {
            return false;
        }
        return true;
    }
}
