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

import org.apache.parquet.io.api.Converter;
import org.knime.bigdata.fileformats.filehandling.reader.cell.BigDataCell;
import org.knime.core.node.context.DeepCopy;

/**
 * {@link BigDataCell} for reading arrays of elements from repeated group element:
 *
 * <pre>
 *   required group my_list (LIST) {
 *     repeated group list {
 *       optional binary a (UTF8);
 *       optional binary b (UTF8);
 *       optional binary c (UTF8);
 *     }
 *   }
 * </pre>
 *
 * This cell should be used together with {@link RepeatedGroupParquetConverter}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SuppressWarnings("javadoc")
final class RepeatedGroupParquetCell implements BigDataCell, DeepCopy<RepeatedGroupParquetCell> {

    private final ParquetCell m_elementConverter;

    private final List<ParquetCell> m_values = new ArrayList<>();

    private boolean m_isNull = true;

    RepeatedGroupParquetCell(final ParquetCell elementCell) {
        m_elementConverter = elementCell;
    }

    private RepeatedGroupParquetCell(final RepeatedGroupParquetCell toCopy) {
        m_elementConverter = toCopy.m_elementConverter.copy();
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

    public Converter getConverter() {
        return m_elementConverter.getConverter();
    }

    public void startRow() {
        m_values.clear();
        m_isNull = false;
    }

    public void startElement() {
        m_elementConverter.reset();
    }

    public void endElement() {
        m_values.add(m_elementConverter.copy());
    }

    public void endRow() {
        // nothing to do
    }

    public void reset() {
        m_isNull = true;
    }

    @Override
    public RepeatedGroupParquetCell copy() {
        return new RepeatedGroupParquetCell(this);
    }

    @Override
    public String toString() {
        return m_isNull ? "null" : m_values.stream()//
            .map(Object::toString)//
            .collect(joining(", ", "[", "]"));
    }

}
