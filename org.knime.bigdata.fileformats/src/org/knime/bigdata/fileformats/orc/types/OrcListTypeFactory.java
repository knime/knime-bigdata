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
 * History 01.05.2018 ("Mareike Hoeger, KNIME GmbH, Konstanz, Germany"): created
 */
package org.knime.bigdata.fileformats.orc.types;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.orc.TypeDescription;
import org.knime.core.data.DataCell;
import org.knime.core.data.collection.CollectionCellFactory;
import org.knime.core.data.collection.ListCell;

/**
 * Factory for the ORC lists.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class OrcListTypeFactory {

    private OrcListTypeFactory() {
        throw new IllegalStateException("Static factory class");
    }

    /**
     * Creates a ORC list type object.
     *
     * @param subtype type of the elements in the list
     * @return the OrcListType
     */
    public static OrcListType create(final OrcType<ColumnVector> subtype) {
        return new OrcListType(subtype);
    }

    /**
     * Type for ORC List.
     *
     * @author "Mareike Hoeger, KNIME GmbH, Konstanz, Germany"
     */
    public static class OrcListType extends AbstractOrcType<ListColumnVector> {

        private final OrcType<ColumnVector> m_subtype;

        /**
         * Creates a list Type with the given type of elements.
         *
         * @param subtype the type of the elements in the list
         */
        public OrcListType(final OrcType<ColumnVector> subtype) {
            super(TypeDescription.createList(subtype.getTypeDescription()));
            m_subtype = subtype;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected DataCell readValueNonNull(final ListColumnVector columnVector, final int rowInBatchCorrected) {
            final Collection<DataCell> cells = new ArrayList<>();
            final ColumnVector chil = columnVector.child;
            final long startRow = columnVector.offsets[rowInBatchCorrected];
            final long end = startRow + columnVector.lengths[rowInBatchCorrected];
            for (long i = startRow; i < end; i++) {
                cells.add(m_subtype.readValue(chil, (int) i));
            }

            return CollectionCellFactory.createListCell(cells);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void writeValueNonNull(final ListColumnVector columnVector, final int rowInBatch,
                final DataCell cell) {
            long offset = 0;
            if (rowInBatch != 0) {
                offset = columnVector.offsets[rowInBatch - 1] + columnVector.lengths[rowInBatch - 1] + 1;
            }
            columnVector.offsets[rowInBatch] = offset;
            final int listsize = ((ListCell) cell).size();
            columnVector.child.ensureSize((int) (offset + listsize), true);

            final Iterator<DataCell> iter = ((ListCell) cell).iterator();

            while (iter.hasNext()) {
                m_subtype.writeValue(columnVector.child, (int) offset, iter.next());
                offset++;
            }

            columnVector.lengths[rowInBatch] = ((ListCell) cell).size();
        }
    }

}
