package org.knime.bigdata.fileformats.orc.datatype.mapping;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.orc.TypeDescription;
import org.knime.core.data.convert.map.AbstractCellValueConsumerFactory;
import org.knime.core.data.convert.map.CellValueConsumer;
import org.knime.core.data.convert.map.MappingException;

/**
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 * @param <T>
 *            the return map type
 * @param <K>
 *            the type of the key
 * @param <V>
 *            the type of the value
 * @param <KCV> the type of the key column vector
 * @param <VCV> the type of the value column vector
 */
public class OrcMapValueConsumerFactory<T, K, V, KCV extends ColumnVector, VCV extends ColumnVector>
extends AbstractCellValueConsumerFactory<ORCDestination, T, TypeDescription, ORCParameter> {

    private class MapConsumer implements CellValueConsumer<ORCDestination, Map<K, V>, ORCParameter> {

        private final ORCCellValueConsumer<K, KCV> m_keyConsumer;
        private final ORCCellValueConsumer<V, VCV> m_valueConsumer;

        public MapConsumer(ORCCellValueConsumer<K, KCV> keyConsumer, ORCCellValueConsumer<V, VCV> valueConsumer) {
            m_keyConsumer = keyConsumer;
            m_valueConsumer = valueConsumer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void consumeCellValue(ORCDestination destination, Map<K, V> value, ORCParameter consumerParams)
                throws MappingException {

            final int colIdx = consumerParams.getColumnIndex();
            final MapColumnVector columnVector = (MapColumnVector) destination.getColumnVector(colIdx);
            final int rowIndex = destination.getRowIndex();

            if (value == null) {
                columnVector.noNulls = false;
                columnVector.isNull[rowIndex] = true;
            } else {
                long offset = 0;
                if (rowIndex != 0) {
                    offset = columnVector.offsets[rowIndex - 1] + columnVector.lengths[rowIndex - 1] + 1;
                }
                columnVector.offsets[rowIndex] = offset;

                for (final Map.Entry<K, V> entry : value.entrySet()) {

                    try {
                        m_keyConsumer.writeNonNullValue((KCV) columnVector.keys, (int) offset, entry.getKey());

                        final V val = entry.getValue();
                        if (val == null) {
                            columnVector.values.noNulls = false;
                            columnVector.values.isNull[(int) offset] = true;
                        } else {
                            m_valueConsumer.writeNonNullValue((VCV) columnVector.values, (int) offset, val);
                            offset++;
                        }
                    } catch (Exception e) {
                        throw new MappingException(e);
                    }
                }
                columnVector.lengths[rowIndex] = value.keySet().size();
            }

        }

    }

    ORCCellValueConsumerFactory<K, KCV> m_keyConsumerFactory;

    ORCCellValueConsumerFactory<V, VCV> m_valueConsumerFactory;

    /**
     * Constructs a map consumer factory
     * 
     * @param keyConsumerFactory
     *            factory for the key consumer
     * @param valueConsumerFactroy
     *            factory for the value consumer
     */
    public OrcMapValueConsumerFactory(ORCCellValueConsumerFactory<K, KCV> keyConsumerFactory,
            ORCCellValueConsumerFactory<V, VCV> valueConsumerFactroy) {
        m_keyConsumerFactory = keyConsumerFactory;
        m_valueConsumerFactory = valueConsumerFactroy;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CellValueConsumer<ORCDestination, T, ORCParameter> create() {
        return (CellValueConsumer<ORCDestination, T, ORCParameter>) new MapConsumer(
                m_keyConsumerFactory.getORCConsumer(), m_valueConsumerFactory.getORCConsumer());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final OrcMapValueConsumerFactory<?, ?, ?, ?, ?> other = (OrcMapValueConsumerFactory<?, ?, ?, ?, ?>) obj;
        if (!(other.m_keyConsumerFactory.equals(m_keyConsumerFactory))) {
            return false;
        }
        if (!(other.m_valueConsumerFactory.equals(m_valueConsumerFactory))) {
            return false;
        }
        return super.equals(obj);
    }

    @Override
    public TypeDescription getDestinationType() {
        return TypeDescription.createMap(m_keyConsumerFactory.getDestinationType(),
                m_valueConsumerFactory.getDestinationType());
    }

    @Override
    public String getIdentifier() {
        return getClass().getName() + "(" + m_keyConsumerFactory.getIdentifier() + " ,"
                + m_valueConsumerFactory.getIdentifier() + ")";
    }

    @Override
    public Class<?> getSourceType() {
        return new HashMap<K, V>().getClass();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_keyConsumerFactory == null) ? 0 : m_keyConsumerFactory.hashCode());
        result = prime * result + ((m_valueConsumerFactory == null) ? 0 : m_valueConsumerFactory.hashCode());
        return result;
    }

}
