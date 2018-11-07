package org.knime.bigdata.fileformats.parquet.datatype.mapping;

/**
 * Parquet converter for type mapping
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 * @param <T> the type to convert to
 */
public interface ParquetConverter<T>{

    /**
     * @return the Object of type {@code T}
     */
    public T readObject();

    /**
     * resets the value in this converter
     */
    public void resetValue();
}
