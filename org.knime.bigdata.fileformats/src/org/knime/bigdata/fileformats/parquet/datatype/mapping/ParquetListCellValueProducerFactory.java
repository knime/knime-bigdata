package org.knime.bigdata.fileformats.parquet.datatype.mapping;

import java.lang.reflect.Array;

import org.knime.core.data.convert.map.CellValueProducer;
import org.knime.core.data.convert.map.MappingException;

/**
 * ListProducer factory for Parquet type mapping.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 * @param <T> the list type
 * @param <E> the element type
 */
public class ParquetListCellValueProducerFactory<T, E> extends ParquetCellValueProducerFactory<T> {

    private ParquetCellValueProducerFactory<E> m_elementProducerFactory;

    /**
     * Constructs a CellFactory for Parquet lists
     *
     * @param externalType the external type of the lists element
     * @param destType the destination type of the lists element
     * @param elementProducerFactory the element producer factory
     */
    @SuppressWarnings("unchecked")
    public ParquetListCellValueProducerFactory(final ParquetType externalType, final Class<?> destType,
        final ParquetCellValueProducerFactory<E> elementProducerFactory) {
        super(externalType, destType,
            (ParquetCellValueProducer<T>)new ToCollectionProducer<>(elementProducerFactory.parquetProducer()));
        m_elementProducerFactory = elementProducerFactory;
    }

    /**
     * Constructs a list producer factory.
     *
     * @param elementProducerFactory the producer factory for the list elements
     */
    public void setelementProducerFactory(final ParquetCellValueProducerFactory<E> elementProducerFactory) {
        m_elementProducerFactory = elementProducerFactory;
    }

    private static class ToCollectionProducer<T, E> extends ParquetCellValueProducer<T> {

        final ParquetCellValueProducer<E> m_elementProducer;

        public ToCollectionProducer(final ParquetCellValueProducer<E> elementProducer) {
            m_elementProducer = elementProducer;
        }

        @Override
        protected ParquetConverter<T> createConverter() {
            return new ParquetListConverter<>(m_elementProducer.createConverter());
        }
    }

    @Override
    public Class<?> getDestinationType() {
        return Array.newInstance(m_elementProducerFactory.getDestinationType(), 0).getClass();
    }

    @Override
    public ParquetType getSourceType() {
        ParquetType elementType = m_elementProducerFactory.getSourceType();

        return ParquetType.createListType(elementType);
    }

    @Override
    public String getIdentifier() {
        return getClass().getName() + "(" + m_elementProducerFactory.getIdentifier() + ")";
    }

    @Override
    public CellValueProducer<ParquetSource, T, ParquetParameter> create() {

        return new CellValueProducer<ParquetSource, T, ParquetParameter>() {

            @Override
            public T produceCellValue(final ParquetSource source, final ParquetParameter params)
                throws MappingException {
                return parquetProducer().getConverter(params.getIndex()).readObject();
            }

        };
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

        final ParquetListCellValueProducerFactory<?, ?> other = (ParquetListCellValueProducerFactory<?, ?>)obj;
        if (!other.m_elementProducerFactory.equals(m_elementProducerFactory)) {
            return false;
        } else {
            return super.equals(obj);
        }

    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_elementProducerFactory == null) ? 0 : m_elementProducerFactory.hashCode());
        return result;
    }

    @Override
    public ParquetListCellValueProducerFactory<T, E> clone() throws CloneNotSupportedException {
        ParquetCellValueProducerFactory<E> clonedProducer = m_elementProducerFactory.clone();
        return new ParquetListCellValueProducerFactory<T, E>(getSourceType(), getDestinationType(), clonedProducer);
    }
}
