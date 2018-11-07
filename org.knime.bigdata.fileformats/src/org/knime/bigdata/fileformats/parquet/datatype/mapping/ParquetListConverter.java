package org.knime.bigdata.fileformats.parquet.datatype.mapping;

import java.util.ArrayList;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;

/**
 * Converter for lists in Parquet.
 * 
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 * @param <T>
 *            the list type
 * @param <E>
 *            the element type
 */
public class ParquetListConverter<T, E> extends GroupConverter implements ParquetConverter<T> {

    ArrayList<E> m_list = new ArrayList<>();

    ParquetConverter<E> m_converter;

    /**
     * Constructs a list converter with the given element converter
     * 
     * @param converter
     *            the element converter
     */
    public ParquetListConverter(ParquetConverter<E> converter) {
        m_converter = converter;
    }

    /**
     * @return the Object of type {@code T}
     */
    @Override
    @SuppressWarnings("unchecked")
    public T readObject() {
        return m_list == null ? null : (T) m_list.toArray();
    }

    /**
     * resets the value in this converter
     */
    @Override
    public void resetValue() {
        m_list = null;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return new GroupConverter() {

            @Override
            public void start() {
                m_converter.resetValue();
            }

            @Override
            public Converter getConverter(int fieldIndex) {
                return (Converter) m_converter;
            }

            @Override
            public void end() {
                m_list.add(m_converter.readObject());
            }
        };
    }

    @Override
    public void start() {
        m_list = new ArrayList<>();
    }

    @Override
    public void end() {
        // Nothing to do
    }
}
