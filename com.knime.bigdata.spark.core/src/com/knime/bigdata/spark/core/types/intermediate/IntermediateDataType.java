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
package com.knime.bigdata.spark.core.types.intermediate;

import java.io.Serializable;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 * Class that acts as an intermediate type between KNIME data types and Spark data types.
 * This class also contains an intermediate type for all Spark DataTypes for all available Spark versions.
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class IntermediateDataType implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String m_typeId; // double, boolean, array/collection, map

    /**
     * @param typeId the unique id of the Spark data type
     */
    public IntermediateDataType(final String typeId) {
        m_typeId = typeId;
    }

    /**
     * @return the typeId
     */
    public String getTypeId() {
        return m_typeId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_typeId == null) ? 0 : m_typeId.hashCode());
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
        IntermediateDataType other = (IntermediateDataType)obj;
        if (m_typeId == null) {
            if (other.m_typeId != null) {
                return false;
            }
        } else if (!m_typeId.equals(other.m_typeId)) {
            return false;
        }
        return true;
    }



}
