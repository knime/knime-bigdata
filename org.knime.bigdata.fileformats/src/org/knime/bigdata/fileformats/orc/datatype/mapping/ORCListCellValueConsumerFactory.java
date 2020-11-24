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
 *   Sep 11, 2018 (Mareike Höger): created
 */

package org.knime.bigdata.fileformats.orc.datatype.mapping;

import java.lang.reflect.Array;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.orc.TypeDescription;
import org.knime.core.data.convert.map.AbstractCellValueConsumerFactory;
import org.knime.core.data.convert.map.CellValueConsumer;
import org.knime.core.data.convert.map.MappingException;

/**
 * Factory for List cell to ORC List
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 * @param <T> the list type
 * @param <E> the element type
 * @param <CV> the type of the column vector
 */
public class ORCListCellValueConsumerFactory<T, E, CV extends ColumnVector>
    extends AbstractCellValueConsumerFactory<ORCDestination, T, TypeDescription, ORCParameter> {

    private class ListConsumer<CCV extends ColumnVector>
        implements CellValueConsumer<ORCDestination, E[], ORCParameter> {

        ORCCellValueConsumer<E, CCV> m_elementconsumer;

        public ListConsumer(final ORCCellValueConsumer<E, CCV> orcConsumer) {
            m_elementconsumer = orcConsumer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void consumeCellValue(final ORCDestination destination, final E[] value, final ORCParameter consumerParams)
            throws MappingException {

            final int colIdx = consumerParams.getColumnIndex();
            final ListColumnVector columnVector = (ListColumnVector)destination.getColumnVector(colIdx);
            final int rowIndex = destination.getRowIndex();

            long offset = 0;
            if (rowIndex != 0) {
                offset = columnVector.offsets[rowIndex - 1] + columnVector.lengths[rowIndex - 1] + 1;
            }
            // the offset must be set whether the value is missing or not
            // otherwise the calculation above will result in the offset starting at 0
            // after a missing value is encountered
            columnVector.offsets[rowIndex] = offset;
            if (value == null) {
                columnVector.noNulls = false;
                columnVector.isNull[rowIndex] = true;
                columnVector.lengths[rowIndex] = 0;
            } else {
                final int listsize = value.length;
                columnVector.child.ensureSize((int)(offset + listsize), true);

                for (final E val : value) {
                    if (val == null) {
                        columnVector.child.noNulls = false;
                        columnVector.child.isNull[(int)offset] = true;
                    } else {
                        try {
                            m_elementconsumer.writeNonNullValue((CCV)columnVector.child, (int)offset, val);
                        } catch (Exception e) {
                            throw new MappingException(e);
                        }

                    }
                    offset++;
                }

                columnVector.lengths[rowIndex] = listsize;
            }

        }

    }

    ORCCellValueConsumerFactory<E, CV> m_elementConsumerFactory;

    /**
     * Constructs a element List consumer
     *
     * @param elementConsumerFactory the factory for the element consumer
     */
    public ORCListCellValueConsumerFactory(final ORCCellValueConsumerFactory<E, CV> elementConsumerFactory) {
        super();
        m_elementConsumerFactory = elementConsumerFactory;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public CellValueConsumer<ORCDestination, T, ORCParameter> create() {

        return (CellValueConsumer<ORCDestination, T, ORCParameter>)new ListConsumer<>(
            m_elementConsumerFactory.getORCConsumer());
    }

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

        final ORCListCellValueConsumerFactory<?, ?, ?> other = (ORCListCellValueConsumerFactory<?, ?, ?>)obj;
        if (!other.m_elementConsumerFactory.equals(m_elementConsumerFactory)) {
            return false;
        } else {
            return super.equals(obj);
        }

    }

    @Override
    public TypeDescription getDestinationType() {

        return TypeDescription.createList(m_elementConsumerFactory.getDestinationType());
    }

    @Override
    public String getIdentifier() {
        return getClass().getName() + "(" + m_elementConsumerFactory.getIdentifier() + ")";
    }

    @Override
    public Class<?> getSourceType() {
        return Array.newInstance(m_elementConsumerFactory.getSourceType(), 0).getClass();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_elementConsumerFactory == null) ? 0 : m_elementConsumerFactory.hashCode());
        return result;
    }
}
