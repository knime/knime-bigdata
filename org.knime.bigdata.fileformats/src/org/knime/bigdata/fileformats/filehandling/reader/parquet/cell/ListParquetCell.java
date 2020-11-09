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
 *   Oct 1, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.parquet.cell;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;

/**
 * {@link ParquetCell} for reading lists.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ListParquetCell extends GroupConverter implements ParquetCell {

    private final ParquetCell m_elementConverter;

    private boolean m_isNull = true;

    private final List<ParquetCell> m_values = new ArrayList<>();


    ListParquetCell(final ParquetCell elementConverter) {
        m_elementConverter = elementConverter;
    }

    private ListParquetCell(final ListParquetCell toCopy) {
        m_elementConverter = toCopy.m_elementConverter.copy();
        m_isNull = toCopy.m_isNull;
        m_values.addAll(toCopy.m_values);
    }

    @Override
    public Converter asConverter() {
        return this;
    }

    @SuppressWarnings("unchecked")
    private <T> T getArray(final Class<T> valueClass) {
        final Class<?> elementClass = valueClass.getComponentType();
        final Object[] array = m_values.stream().map(c -> c.getObj(elementClass)).toArray();
        return (T) array;
    }

    @Override
    public String getString() {
        return m_values.stream().map(ParquetCell::getString).collect(Collectors.joining(", ", "[", "]"));
    }

    @Override
    public boolean isNull() {
        return m_isNull;
    }

    @Override
    public void reset() {
        m_isNull = true;
    }

    @Override
    public Converter getConverter(final int fieldIndex) {
        return new GroupConverter() {

            @Override
            public Converter getConverter(final int innerFieldIndex) {
                return m_elementConverter.asConverter();
            }

            @Override
            public void start() {
                m_elementConverter.reset();
            }

            @Override
            public void end() {
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
    public <T> T getObj(final Class<T> expectedValueClass) {
        return getArray(expectedValueClass);
    }

    @Override
    public ListParquetCell copy() {
        return new ListParquetCell(this);
    }

}
