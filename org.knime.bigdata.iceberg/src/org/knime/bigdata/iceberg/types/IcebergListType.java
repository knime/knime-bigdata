/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   2025-05-27 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.iceberg.types;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.knime.core.data.DataType;
import org.knime.core.data.collection.ListCell;

/**
 * A {@link IcebergDataType} that represents a list.
 *
 * Based on {@code ListKnimeType} of the Big Data Reader.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public final class IcebergListType implements IcebergDataType {

    private final IcebergDataType m_elementType;

    private final DataType m_defaultDataType;

    /**
     * Constructor.
     *
     * @param elementType the {@link IcebergDataType} of the elements in this list.
     */
    IcebergListType(final IcebergDataType elementType) {
        m_elementType = elementType;
        m_defaultDataType = ListCell.getCollectionType(elementType.getDefaultDataType());
    }

    public static IcebergListType of(final IcebergDataType elementType) {
        return new IcebergListType(elementType);
    }

    @Override
    public DataType getDefaultDataType() {
        return m_defaultDataType;
    }

    /**
     * Returns the element type, possible a list as well.
     *
     * @return the type of element in this list (can be a nested list)
     */
    public IcebergDataType getElementType() {
        return m_elementType;
    }

    static IcebergDataType toExternalType(final String serializedType) {
        final var elementTypeName = serializedType.substring("list(".length(), serializedType.length() - 1);
        final var elementType = IcebergPrimitiveType.toExternalType(elementTypeName);
        return new IcebergListType(elementType);
    }

    @Override
    public String toSerializableType() {
        return "list(" + m_elementType.toSerializableType() + ")";
    }

    @Override
    public String toString() {
        return "List(" + m_elementType + ")";
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof IcebergListType listType) {
            return equals(listType);
        } else {
            return false;
        }
    }

    private boolean equals(final IcebergListType other) {
        return m_elementType.equals(other.m_elementType) //
            && m_defaultDataType.equals(other.m_defaultDataType);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder() //
            .append(m_elementType) //
            .append(m_defaultDataType) //
            .toHashCode();
    }

}
