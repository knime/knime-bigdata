package org.knime.bigdata.fileformats.parquet.datatype.mapping;

import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * Converter for Primitive Parquet types
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 * @param <T> the type to convert to
 */
public class ParquetPrimitiveConverter<T> extends PrimitiveConverter implements ParquetConverter<T> {

    protected T m_value;

    /**
     * @return the Object of type {@code T}
     */
    @Override
    public T readObject() {
        return m_value;
    }

    /**
     * resets the value in this converter
     */
    @Override
    public void resetValue() {
        m_value = null;
    }

}
