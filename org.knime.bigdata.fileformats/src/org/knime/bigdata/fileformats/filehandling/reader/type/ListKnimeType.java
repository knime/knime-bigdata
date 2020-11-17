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
 *   Nov 5, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.type;

import static java.util.stream.Collectors.toSet;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.Set;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.knime.core.data.DataType;
import org.knime.core.data.collection.ListCell;

/**
 * A {@link KnimeType} representing homogenous lists.</br>
 * Theoretically supports nesting.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ListKnimeType implements KnimeType {

    private final KnimeType m_elementType;

    private final DataType m_defaultDataType;

    private final Set<Class<?>> m_supportedJavaClasses;

    /**
     * Constructor.
     *
     * @param elementType the {@link KnimeType} of the elements in this list.
     */
    public ListKnimeType(final KnimeType elementType) {
        m_elementType = elementType;
        m_defaultDataType = ListCell.getCollectionType(elementType.getDefaultDataType());
        m_supportedJavaClasses =
            Collections.unmodifiableSet(elementType.getSupportedJavaClasses().stream().map(ListKnimeType::getArrayClass)//
                .collect(toSet()));
    }

    private static Class<? extends Object> getArrayClass(final Class<?> elementType) {
        return Array.newInstance(elementType, 0).getClass();
    }

    @Override
    public boolean isList() {
        return true;
    }

    @Override
    public ListKnimeType asListType() {
        return this;
    }

    @Override
    public Set<Class<?>> getSupportedJavaClasses() {
        return m_supportedJavaClasses;
    }

    @Override
    public DataType getDefaultDataType() {
        return m_defaultDataType;
    }

    @Override
    public PrimitiveKnimeType asPrimitiveType() {
        throw new IllegalStateException("This is a list type.");
    }

    KnimeType getElementType() {
        return m_elementType;
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
        if (obj instanceof ListKnimeType) {
            return equals((ListKnimeType)obj);
        } else {
            return false;
        }
    }

    private boolean equals(final ListKnimeType other) {
        return m_elementType.equals(other.m_elementType)//
            && m_supportedJavaClasses.equals(other.m_supportedJavaClasses)//
            && m_defaultDataType.equals(other.m_defaultDataType);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()//
            .append(m_elementType)//
            .append(m_supportedJavaClasses)//
            .append(m_defaultDataType)//
            .toHashCode();
    }

}