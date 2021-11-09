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
 *   Nov 14, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet.cell;

import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * {@link ParquetCell} for reading legacy lists format without the repeated group element:
 *
 * <pre>
 *   required group my_list (LIST) {
 *     repeated binary element (UTF8);
 *   }
 * </pre>
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @author Sascha Wolke, KNIME GmbH
 */
final class ListParquetLegacyCell extends GroupConverter implements ParquetCell {

    private final ParquetCell m_elementConverter;

    private final PrimitiveConverter m_primitiveElementConverter;

    private final List<ParquetCell> m_values = new ArrayList<>();

    private boolean m_isNull = true;

    ListParquetLegacyCell(final ParquetCell elementCell) {
        m_elementConverter = elementCell;
        m_primitiveElementConverter = m_elementConverter.getConverter().asPrimitiveConverter();
    }

    private ListParquetLegacyCell(final ListParquetLegacyCell toCopy) {
        m_elementConverter = toCopy.m_elementConverter.copy();
        m_primitiveElementConverter = m_elementConverter.getConverter().asPrimitiveConverter();
        m_isNull = toCopy.m_isNull;
        m_values.addAll(toCopy.m_values);
    }

    @Override
    public boolean isNull() {
        return m_isNull;
    }

    @Override
    public String getString() {
        return m_values.stream()//
            .map(ParquetCell::getString)//
            .collect(joining(", ", "[", "]"));
    }

    @SuppressWarnings("unchecked")
    private <T> T getArray(final Class<T> valueClass) {
        final Class<?> elementClass = valueClass.getComponentType();
        final Object[] array = m_values.stream()//
            .map(c -> c.isNull() ? null : c.getObj(elementClass))//
            .toArray();
        return (T)array;
    }

    @Override
    public <T> T getObj(final Class<T> expectedClass) {
        assert !isNull() : "Getter should only be called if isNull() returns false.";
        if (expectedClass == String.class) {
            return expectedClass.cast(getString());
        } else {
            return getArray(expectedClass);
        }
    }

    @Override
    public Converter getConverter(final int fieldIndex) {
        return new PrimitiveConverter() { // NOSONAR

            @Override
            public void addBinary(final Binary value) {
                m_elementConverter.reset();
                m_primitiveElementConverter.addBinary(value);
                m_values.add(m_elementConverter.copy());
            }

            @Override
            public void addBoolean(final boolean value) {
                m_elementConverter.reset();
                m_primitiveElementConverter.addBoolean(value);
                m_values.add(m_elementConverter.copy());
            }

            @Override
            public void addDouble(final double value) {
                m_elementConverter.reset();
                m_primitiveElementConverter.addDouble(value);
                m_values.add(m_elementConverter.copy());
            }

            @Override
            public void addFloat(final float value) {
                m_elementConverter.reset();
                m_primitiveElementConverter.addFloat(value);
                m_values.add(m_elementConverter.copy());
            }

            @Override
            public void addInt(final int value) {
                m_elementConverter.reset();
                m_primitiveElementConverter.addInt(value);
                m_values.add(m_elementConverter.copy());
            }

            @Override
            public void addLong(final long value) {
                m_elementConverter.reset();
                m_primitiveElementConverter.addLong(value);
                m_values.add(m_elementConverter.copy());
            }
        };
    }

    @Override
    public void start() {
        m_values.clear();
        m_isNull = false;
    }

    @Override
    public void end() {
        // nothing to do
    }

    @Override
    public ParquetCell copy() {
        return new ListParquetLegacyCell(this);
    }

    @Override
    public Converter getConverter() {
        return this;
    }

    @Override
    public void reset() {
        m_isNull = true;
    }

    @Override
    public String toString() {
        return m_isNull ? "null" : m_values.stream()//
            .map(Object::toString)//
            .collect(joining(", ", "[", "]"));
    }

}
