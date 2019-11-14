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
 *   09.10.2018 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */

package org.knime.bigdata.fileformats.parquet.datatype.mapping;

import org.apache.parquet.io.api.Converter;
import org.knime.core.data.convert.map.SimpleCellValueProducerFactory;

/**
 * Factory for Parquet cell value producer.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 * @param <T> the type to produce
 */
public class ParquetCellValueProducerFactory<T>
extends SimpleCellValueProducerFactory<ParquetSource, ParquetType, T, ParquetParameter> implements Cloneable {

    private final ParquetCellValueProducer<T> m_parquetProducer;


    /**
     * COnstructs a producer factory
     * @param externalType the external type to produce from
     * @param destType the destination type
     * @param producer the cell value producer
     */
    public ParquetCellValueProducerFactory(final ParquetType externalType, final Class<?> destType,
            final ParquetCellValueProducer<T> producer) {
        super(externalType, destType, (c, p) -> producer.getConverter(p.getIndex()).readObject());
        m_parquetProducer = producer;
    }

    @SuppressWarnings("unchecked")
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
        final  ParquetCellValueProducerFactory<T> other = ( ParquetCellValueProducerFactory<T> ) obj;
        if (!(other.m_parquetProducer.equals(m_parquetProducer))) {
            return false;
        }
        return super.equals(obj);
    }

    /**
     * Returns the converter for the given column index
     * @param colIndex the index of the column
     * @return the converter
     */
    public Converter getConverter(final int colIndex) {
        return (Converter) m_parquetProducer.getConverter(colIndex);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_parquetProducer == null) ? 0 : m_parquetProducer.hashCode());
        return result;
    }

    /**
     * Resets the producer convertes
     */
    public void resetConverters() {
        m_parquetProducer.resetConverters();
    }

    /**
     * @return the m_parquetProducer
     */
    protected ParquetCellValueProducer<T> parquetProducer() {
        return m_parquetProducer;
    }


    @Override
    public ParquetCellValueProducerFactory<T> clone() throws CloneNotSupportedException{
        ParquetCellValueProducer<T> clonedProducer = m_parquetProducer.clone();
        return new ParquetCellValueProducerFactory<T>(getSourceType(), getDestinationType(), clonedProducer);
    }
}
