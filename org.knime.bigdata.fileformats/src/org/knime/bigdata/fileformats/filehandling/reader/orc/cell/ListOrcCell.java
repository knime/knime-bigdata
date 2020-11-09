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
 *   Nov 7, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.orc.cell;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;

/**
 * A special {@link OrcCell} for reading from {@link ListColumnVector}.</br>
 * It holds an OrcCell for reading its elements which therefore must be homogenous.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ListOrcCell implements OrcCell {

    private final OrcCell m_elementCell;

    private final List<Object> m_values = new ArrayList<>();

    private ListColumnVector m_vector;

    private int m_idx;

    ListOrcCell(final OrcCell elementCell) {
        m_elementCell = elementCell;
    }

    private <A> A getArray(final Class<A> arrayClass) {
        assert arrayClass.isArray() : "Only arrays are supported by ListOrcCell.";
        m_values.clear();
        final Class<?> elementClass = arrayClass.getComponentType();
        final long start = m_vector.offsets[m_idx];
        final long length = m_vector.lengths[m_idx];
        final long end = start + length;
        for (long i = start; i < end; i++) {
            m_elementCell.setIndexInColumn((int)i);
            m_values.add(m_elementCell.isNull() ? null : m_elementCell.getObj(elementClass));
        }
        @SuppressWarnings("unchecked")
        final A array = (A)m_values.toArray();
        return array;
    }

    @Override
    public void setColVector(final ColumnVector colVector) {
        m_vector = (ListColumnVector)colVector;
        m_elementCell.setColVector(m_vector.child);
    }

    @Override
    public void setIndexInColumn(final int idxInColumn) {
        m_idx = idxInColumn;
    }

    @Override
    public boolean isNull() {
        return m_vector == null || (!m_vector.noNulls && m_vector.isNull[m_idx]);
    }

    @Override
    public String getString() {
        return m_values.stream()//
            .map(Object::toString)//
            .collect(Collectors.joining(", ", "[", "]"));
    }

    @Override
    public <T> T getObj(final Class<T> expectedClass) {
        if (expectedClass.isArray()) {
            return getArray(expectedClass);
        } else if (expectedClass == String.class) {
            return expectedClass.cast(getString());
        }
        throw new IllegalArgumentException(
            String.format("The value class %s is not supported by ListOrcCell.", expectedClass));
    }

}
