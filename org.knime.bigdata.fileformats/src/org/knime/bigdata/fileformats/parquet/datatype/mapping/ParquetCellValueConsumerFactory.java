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
 *   Oct 9, 2018 (Mareike Höger): created
 */

package org.knime.bigdata.fileformats.parquet.datatype.mapping;

import org.apache.parquet.io.api.RecordConsumer;
import org.knime.core.data.convert.map.MappingException;
import org.knime.core.data.convert.map.SimpleCellValueConsumerFactory;

/**
 * Parquet call value consumer factory
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 * 
 * @param <T> the input type for the consumer
 *
 */
public class ParquetCellValueConsumerFactory<T>
        extends SimpleCellValueConsumerFactory<ParquetDestination, T, ParquetType, ParquetParameter> {

    private final ParquetCellValueConsumer<T> m_parquetConsumer;

    /**
     * Constructs a Parquet call value consumer factory
     * 
     * @param sourceType
     *            the source type to consume
     * @param externalType
     *            the external type to produce
     * @param consumer
     *            the consumer to use
     */
    public ParquetCellValueConsumerFactory(Class<?> sourceType, ParquetType externalType,
            ParquetCellValueConsumer<T> consumer) {
        super(sourceType, externalType, (c, v, p) -> {

            final RecordConsumer recordConsumer = c.getRecordConsumer();
            final int index = p.getIndex();

            if (v != null) {
                try {
                    recordConsumer.startField(externalType.getName(), index);

                    consumer.writeNonNullValue(recordConsumer, v);

                    recordConsumer.endField(externalType.getName(), index);
                } catch (Exception e) {
                    throw new MappingException(e);
                }
            }
        });
        m_parquetConsumer = consumer;
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
        final ParquetCellValueConsumerFactory<?> other = (ParquetCellValueConsumerFactory<?>) obj;
        if (!(other.m_parquetConsumer.equals(m_parquetConsumer))) {
            return false;
        }
        return super.equals(obj);
    }

    /**
     * @return the Parquet consumer for this factory
     */
    public ParquetCellValueConsumer<T> getParquetConsumer() {
        return m_parquetConsumer;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_parquetConsumer == null) ? 0 : m_parquetConsumer.hashCode());
        return result;
    }

}
