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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.TypeDescription;
import org.knime.core.data.convert.map.SimpleCellValueProducerFactory;

/**
 * ORC cell value producer factory
 * 
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 * @param <T>
 *            the Type of the produced object
 * @param <CV> the type of the column vector
 */
public class ORCCellValueProducerFactory<T, CV extends ColumnVector>
extends SimpleCellValueProducerFactory<ORCSource, TypeDescription, T, ORCParameter> {

    private final ORCCellValueProducer<T, CV> m_orcProducer;

    /**
     * Constructs a ORC cell value factory
     * 
     * @param externalType
     *            the type to produce from
     * @param destType
     *            the type to produce
     * @param producer
     *            the producer
     */
    public ORCCellValueProducerFactory(TypeDescription externalType, Class<?> destType,
            ORCCellValueProducer<T, CV> producer) {

        super(externalType, destType, (c, p) -> {

            final int colIdx = p.getColumnIndex();
            @SuppressWarnings("unchecked")
            final CV columnVector = (CV) c.getColumVector(colIdx);
            int rowInBatch = (int) c.getRowIndex();
            if (columnVector.isRepeating) {
                rowInBatch = 0;
            }
            T object = null;
            if (columnVector.noNulls || !columnVector.isNull[rowInBatch]) {
                object = producer.readObject(columnVector, rowInBatch);
            }
            return object;

        });

        m_orcProducer = producer;
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
        final ORCCellValueProducerFactory<?,?>other = (ORCCellValueProducerFactory<?,?>) obj;
        if(!(other.m_orcProducer.equals(m_orcProducer))) {
            return false;
        }
        return super.equals(obj);
    }

    /**
     * @return the m_producer
     */
    public ORCCellValueProducer<T, CV> getORCproducer() {
        return m_orcProducer;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_orcProducer == null) ? 0 : m_orcProducer.hashCode());
        return result;
    }
}
