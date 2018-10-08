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
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.orc.TypeDescription;
import org.knime.core.data.convert.map.CellValueProducer;
import org.knime.core.data.convert.map.CellValueProducerFactory;
import org.knime.core.data.convert.map.MappingException;

/**
 * Producer factory for ORC list
 * 
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 * @param <T>
 *            the return type of the producer
 * @param <F>
 *            the element type in the list
 * @param <CV> the type of the column vector
 */
public class ORCListCellValueProducerFactory<T, F, CV extends ColumnVector>
implements CellValueProducerFactory<ORCSource, TypeDescription, T, ORCParameter> {

    private static class ToCollectionProducer<E, CCV extends ColumnVector> implements CellValueProducer<ORCSource, E[], ORCParameter> {

        final ORCCellValueProducer<E, CCV> m_elementConverter;

        public ToCollectionProducer(ORCCellValueProducer<E, CCV> elementProducer) {
            m_elementConverter = elementProducer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public E[] produceCellValue(ORCSource source, ORCParameter params) throws MappingException {
            final int colIdx = params.getColumnIndex();
            final ListColumnVector columnVector = (ListColumnVector) source.getColumVector(colIdx);
            final int rowInBatch = (int) source.getRowIndex();
            E[] array = null;
            if (columnVector.noNulls || !columnVector.isNull[rowInBatch]) {
                final Collection<E> list = new ArrayList<>();
                final CCV chil = (CCV) columnVector.child;
                final long startRow = columnVector.offsets[rowInBatch];
                final long end = startRow + columnVector.lengths[rowInBatch];
                for (long i = startRow; i < end; i++) {
                    E element = null;
                    if (chil.noNulls || !chil.isNull[(int) i]) {
                        element = m_elementConverter.readObject(chil, (int) i);
                    }

                    list.add(element);
                }

                array = (E[]) list.toArray();
            }
            return array;
        }
    }

    private final ORCCellValueProducerFactory<F, CV> m_elementFactory;

    /**
     * Constructs a ORC list producer factory
     * 
     * @param elementFactory
     *            the factory for the element producer
     */
    public ORCListCellValueProducerFactory(ORCCellValueProducerFactory<F, CV> elementFactory) {
        m_elementFactory = elementFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CellValueProducer<ORCSource, T, ORCParameter> create() {
        final ORCCellValueProducer<F, CV> elementProducer = m_elementFactory.getORCproducer();
        return (CellValueProducer<ORCSource, T, ORCParameter>) new ToCollectionProducer<>(elementProducer);
    }

    @Override
    public Class<?> getDestinationType() {
        return Array.newInstance(m_elementFactory.getDestinationType(), 0).getClass();
    }

    @Override
    public String getIdentifier() {
        return getClass().getName() + "(" + m_elementFactory.getIdentifier() + ")";
    }

    @Override
    public TypeDescription getSourceType() {
        return TypeDescription.createList(m_elementFactory.getSourceType());
    }

}
