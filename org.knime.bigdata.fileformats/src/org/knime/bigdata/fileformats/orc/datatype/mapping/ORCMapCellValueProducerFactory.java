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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.orc.TypeDescription;
import org.knime.core.data.convert.map.CellValueProducer;
import org.knime.core.data.convert.map.CellValueProducerFactory;
import org.knime.core.data.convert.map.MappingException;

/**
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 * @param <T>
 *            The output type
 * @param <K>
 *            The key type of the map
 * @param <V>
 *            The value type of the map
 * @param <KCV> the type of the key column vector
 * @param <VCV> the type of the value column vector
 */
public class ORCMapCellValueProducerFactory<T, K, V, KCV extends ColumnVector, VCV extends ColumnVector>
implements CellValueProducerFactory<ORCSource, TypeDescription, T, ORCParameter> {

    private static class ToMapProducer<E, A, ECV extends ColumnVector, ACV extends ColumnVector> 
    implements CellValueProducer<ORCSource, Map<E, A>, ORCParameter> {

        final ORCCellValueProducer<E, ECV> m_keyProducer;
        final ORCCellValueProducer<A, ACV> m_valueProducer;

        public ToMapProducer(final ORCCellValueProducer<E, ECV> keyProducer, 
                final ORCCellValueProducer<A, ACV> valueProducer) {
            m_keyProducer = keyProducer;
            m_valueProducer = valueProducer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map<E, A> produceCellValue(ORCSource source, ORCParameter params) throws MappingException {
            final int colIdx = params.getColumnIndex();
            final MapColumnVector columnVector = (MapColumnVector) source.getColumVector(colIdx);
            final int rowInBatch = (int) source.getRowIndex();
            final Map<E, A> map = new HashMap<>();
            if (columnVector.noNulls || !columnVector.isNull[rowInBatch]) {
                final ECV keyVector = (ECV) columnVector.keys;
                final ACV valueVector = (ACV) columnVector.values;
                final long startRow = columnVector.offsets[rowInBatch];
                final long end = startRow + columnVector.lengths[rowInBatch];
                for (long i = startRow; i < end; i++) {
                    final E key = m_keyProducer.readObject(keyVector, (int) i);
                    final A value = m_valueProducer.readObject(valueVector, (int) i);

                    map.put(key, value);
                }
            }
            return map;
        }
    }

    private final ORCCellValueProducerFactory<K, KCV> m_keyFactory;

    private final ORCCellValueProducerFactory<V, VCV> m_valueFactory;

    /**
     * The Producer Factory for ORC Maps
     * 
     * @param keyFactory
     *            the factory for the key objects producer
     * @param valueFactory
     *            the factory for the value object producer
     * 
     */
    public ORCMapCellValueProducerFactory(ORCCellValueProducerFactory<K, KCV> keyFactory,
            ORCCellValueProducerFactory<V, VCV> valueFactory) {
        m_keyFactory = keyFactory;
        m_valueFactory = valueFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CellValueProducer<ORCSource, T, ORCParameter> create() {
        return (CellValueProducer<ORCSource, T, ORCParameter>) new ToMapProducer<>(m_keyFactory.getORCproducer(),
                m_valueFactory.getORCproducer());
    }

    @Override
    public Class<?> getDestinationType() {
        return new HashMap<K, V>().getClass();
    }

    @Override
    public String getIdentifier() {
        return getClass().getName() + "(" + m_keyFactory.getIdentifier() + " ," + m_valueFactory.getIdentifier() + ")";
    }

    @Override
    public TypeDescription getSourceType() {
        return TypeDescription.createMap(m_keyFactory.getSourceType(), m_valueFactory.getSourceType());
    }

}
