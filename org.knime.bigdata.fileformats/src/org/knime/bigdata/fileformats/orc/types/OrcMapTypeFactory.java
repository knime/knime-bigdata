/*
 * ------------------------------------------------------------------------
 *
 * Copyright by KNIME AG, Zurich, Switzerland Website: http://www.knime.com;
 * Email: contact@knime.com
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, Version 3, as published by the
 * Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, see <http://www.gnu.org/licenses>.
 *
 * Additional permission under GNU GPL version 3 section 7:
 *
 * KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs. Hence,
 * KNIME and ECLIPSE are both independent programs and are not derived from each
 * other. Should, however, the interpretation of the GNU GPL Version 3
 * ("License") under any applicable laws result in KNIME and ECLIPSE being a
 * combined program, KNIME AG herewith grants you the additional permission to
 * use and propagate KNIME together with ECLIPSE with only the license terms in
 * place for ECLIPSE applying to ECLIPSE and the GNU GPL Version 3 applying for
 * KNIME, provided the license terms of ECLIPSE themselves allow for the
 * respective use and propagation of ECLIPSE together with KNIME.
 *
 * Additional permission relating to nodes for KNIME that extend the Node
 * Extension (and in particular that are based on subclasses of NodeModel,
 * NodeDialog, and NodeView) and that only interoperate with KNIME through
 * standard APIs ("Nodes"): Nodes are deemed to be separate and independent
 * programs and to not be covered works. Notwithstanding anything to the
 * contrary in the License, the License does not apply to Nodes, you are not
 * required to license Nodes under the License, and you are granted a license to
 * prepare and propagate Nodes, in each case even if such Nodes are propagated
 * with or for interoperation with KNIME. The owner of a Node may freely choose
 * the license terms applicable to such Node, including when such Node is
 * propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History 08.05.2018 ("Mareike Hoeger, KNIME GmbH, Konstanz, Germany"): created
 */
package org.knime.bigdata.fileformats.orc.types;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.orc.TypeDescription;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.core.data.DataCell;
import org.knime.core.data.collection.CollectionCellFactory;
import org.knime.core.data.collection.ListCell;

/**
 * Type factory for the ORC Map type.
 *
 * @author "Mareike Hoeger, KNIME GmbH, Konstanz, Germany"
 */
public class OrcMapTypeFactory {

    private OrcMapTypeFactory() {
        throw new IllegalStateException("Static factory class");
    }

    /**
     * Creates a the ORC map type, with the given key- and value types.
     *
     * @param keyType the key type
     * @param valueType the value type
     * @return a ORC map type
     */
    public static OrcMapType create(final OrcType<ColumnVector> keyType, final OrcType<ColumnVector> valueType) {
        return new OrcMapType(keyType, valueType);
    }

    /**
     * Type for ORC maps.
     *
     * @author "Mareike Hoeger, KNIME GmbH, Konstanz, Germany"
     */
    public static class OrcMapType extends AbstractOrcType<MapColumnVector> {
        private final OrcType<ColumnVector> m_keyType;

        private final OrcType<ColumnVector> m_valueType;

        /**
         * Creates a ORC map type with the given key and value types.
         *
         * @param keyType the type of the keys
         * @param valueType the type of the values
         */
        public OrcMapType(final OrcType<ColumnVector> keyType, final OrcType<ColumnVector> valueType) {
            super(TypeDescription.createMap(keyType.getTypeDescription(), valueType.getTypeDescription()));
            m_keyType = keyType;
            m_valueType = valueType;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected DataCell readValueNonNull(final MapColumnVector columnVector, final int rowInBatchCorrected) {
            final long startRow = columnVector.offsets[rowInBatchCorrected];
            final long end = startRow + columnVector.lengths[rowInBatchCorrected];
            final Collection<DataCell> keyCells = new ArrayList<>();
            final Collection<DataCell> valueCells = new ArrayList<>();
            for (long i = startRow; i < end; i++) {
                keyCells.add(m_keyType.readValue(columnVector.keys, (int) i));
                valueCells.add(m_valueType.readValue(columnVector.values, (int) i));
            }

            final Collection<DataCell> mapTuple = new ArrayList<>();
            mapTuple.add(CollectionCellFactory.createListCell(keyCells));
            mapTuple.add(CollectionCellFactory.createListCell(valueCells));
            return CollectionCellFactory.createListCell(mapTuple);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void writeValueNonNull(final MapColumnVector columnVector, final int rowInBatch,
                final DataCell cell) {
            long offset = 0;
            if (rowInBatch != 0) {
                offset = columnVector.offsets[rowInBatch - 1] + columnVector.lengths[rowInBatch - 1] + 1;
            }
            columnVector.offsets[rowInBatch] = offset;
            if (((ListCell) cell).size() != 2) {
                throw new BigDataFileFormatException("The cell does not cotain a map.");
            }
            final DataCell keys = ((ListCell) cell).get(0);
            final DataCell values = ((ListCell) cell).get(1);
            if (!keys.getType().isCollectionType() || !values.getType().isCollectionType()) {
                throw new BigDataFileFormatException("The cell does not cotain a map.");
            }
            final Iterator<DataCell> keyIter = ((ListCell) keys).iterator();
            final Iterator<DataCell> valueIter = ((ListCell) values).iterator();
            while (keyIter.hasNext() && valueIter.hasNext()) {
                m_keyType.writeValue(columnVector.keys, (int) offset, keyIter.next());
                m_valueType.writeValue(columnVector.values, (int) offset, valueIter.next());
                offset++;
            }
            columnVector.lengths[rowInBatch] = ((ListCell) keys).size();

        }

    }
}
