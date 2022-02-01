package org.knime.bigdata.fileformats.parquet.datatype.mapping;

import org.knime.core.data.convert.map.Source;

/**
 * Source implementation for Parquet type mapping
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 */
public abstract class ParquetSource implements Source<ParquetType> {

    long m_rowIndex = 0;

    ParquetSource() {
    }

    /**
     * @return the current row index
     */
    public long getRowIndex() {
        return m_rowIndex;
    }

    /**
     * switches to the next row
     */
    public void next() {
        m_rowIndex++;
    }

}
