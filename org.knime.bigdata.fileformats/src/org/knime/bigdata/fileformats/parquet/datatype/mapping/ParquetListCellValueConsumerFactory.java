package org.knime.bigdata.fileformats.parquet.datatype.mapping;

import java.lang.reflect.Array;

import org.apache.parquet.io.api.RecordConsumer;
import org.knime.core.data.convert.map.AbstractCellValueConsumerFactory;
import org.knime.core.data.convert.map.CellValueConsumer;
import org.knime.core.data.convert.map.MappingException;

/**
 * List Cell Consumer Factory.
 * 
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 * @param <T> The list type
 * @param <E> the element type
 */
public class ParquetListCellValueConsumerFactory<T, E>
        extends AbstractCellValueConsumerFactory<ParquetDestination, T, ParquetType, ParquetParameter> {

    ParquetCellValueConsumerFactory<E> m_elementConsumerFactory;
    private static final String ELEMENT = "element";

    /**
     * Constructs a cell factory for lists containing types that can be consumed by
     * the given element consumer factory.
     * 
     * @param elementConsumerFactory
     *            the consumer factory for the element consumer
     */
    public ParquetListCellValueConsumerFactory(ParquetCellValueConsumerFactory<E> elementConsumerFactory) {
        this.m_elementConsumerFactory = elementConsumerFactory;
    }

    private class ListConsumer implements CellValueConsumer<ParquetDestination, E[], ParquetParameter> {

        private ParquetCellValueConsumer<E> m_elementConsumer;

        public ListConsumer(ParquetCellValueConsumer<E> parquetConsumer) {
            m_elementConsumer = parquetConsumer;
        }

        @Override
        public void consumeCellValue(ParquetDestination destination, E[] value, ParquetParameter consumerParams)
                throws MappingException {

            RecordConsumer consumer = destination.getRecordConsumer();
            if (value != null) {
                consumer.startField(consumerParams.getField(), consumerParams.getIndex());
                consumer.startGroup();

                consumer.startField("list", 0);

                for (int i = 0; i < value.length; i++) {
                    consumer.startGroup();

                    if (value[i] != null) {
                        try {
                            consumer.startField(ELEMENT, 0);
                            m_elementConsumer.writeNonNullValue(consumer, value[i]);
                            consumer.endField(ELEMENT, 0);
                        }catch (Exception e){
                            throw new MappingException(e);
                        }
                    }

                    consumer.endGroup();
                }

                consumer.endField("list", 0);

                consumer.endGroup();
                consumer.endField(consumerParams.getField(), consumerParams.getIndex());
            }
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    public CellValueConsumer<ParquetDestination, T, ParquetParameter> create() {
        return (CellValueConsumer<ParquetDestination, T, ParquetParameter>) new ListConsumer(
                m_elementConsumerFactory.getParquetConsumer());
    }

    @Override
    public ParquetType getDestinationType() {
        ParquetType elementDestType = m_elementConsumerFactory.getDestinationType();
        return ParquetType.createListType(elementDestType);

    }

    @Override
    public Class<?> getSourceType() {
        return Array.newInstance(m_elementConsumerFactory.getSourceType(), 0).getClass();
    }

    @Override
    public String getIdentifier() {
        return getClass().getName() + "(" + m_elementConsumerFactory.getIdentifier() + ")";
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

        final ParquetListCellValueConsumerFactory<?, ?> other = (ParquetListCellValueConsumerFactory<?, ?>) obj;
        if (!other.m_elementConsumerFactory.equals(m_elementConsumerFactory)) {
            return false;
        } else {
            return super.equals(obj);
        }

    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_elementConsumerFactory == null) ? 0 : m_elementConsumerFactory.hashCode());
        return result;
    }
}
