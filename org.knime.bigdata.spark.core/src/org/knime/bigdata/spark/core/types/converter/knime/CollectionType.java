/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on 30.05.2016 by koetter
 */
package org.knime.bigdata.spark.core.types.converter.knime;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import org.knime.bigdata.spark.core.types.intermediate.IntermediateArrayDataType;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.collection.CollectionCellFactory;
import org.knime.core.data.collection.CollectionDataValue;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.collection.SetCell;
import org.knime.core.data.collection.SetDataValue;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class CollectionType implements KNIMEToIntermediateConverter {

    private KNIMEToIntermediateConverter m_elementConverter;
    private DataType m_type;

    /**
     * @param type optional KNIME {@link DataType}
     * @param elementConverter the {@link KNIMEToIntermediateConverter} of the collection elements
     */
    public CollectionType(final DataType type, final KNIMEToIntermediateConverter elementConverter) {
        m_type = type;
        m_elementConverter = elementConverter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return "Array[" + m_elementConverter.getName() + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return "Array + " + m_elementConverter.getDescription();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntermediateDataType getIntermediateDataType() {
        return new IntermediateArrayDataType(m_elementConverter.getIntermediateDataType());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntermediateDataType[] getSupportedIntermediateDataTypes() {
        throw new UnsupportedOperationException();
//IntermediateDataType[] supportedIntermediateDataTypes = m_elementConverter.getSupportedIntermediateDataTypes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getKNIMEDataType() {
        if (createSet()) {
            return SetCell.getCollectionType(m_elementConverter.getKNIMEDataType());
        }
        return ListCell.getCollectionType(m_elementConverter.getKNIMEDataType());
    }

    /**
     * @return
     */
    private boolean createSet() {
        return m_type != null && m_type.isCompatible(SetDataValue.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataCell convert(final Serializable intermediateTypeObject,
        final KNIMEToIntermediateConverterParameter parameter) {

        if (intermediateTypeObject instanceof Serializable[]) {
            final Serializable[] vals = (Serializable[])intermediateTypeObject;
            final Collection<DataCell> cells = new ArrayList<>(vals.length);
            for (Serializable val : vals) {
                cells.add(m_elementConverter.convert(val, parameter));
            }
            if (createSet()) {
                return CollectionCellFactory.createSetCell(cells);
            }
            return CollectionCellFactory.createListCell(cells);
        }

        return DataType.getMissingCell();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Serializable convert(final DataCell cell, final KNIMEToIntermediateConverterParameter parameter) {
        if (cell instanceof CollectionDataValue) {
            final CollectionDataValue colCell = (CollectionDataValue) cell;
            final Serializable[ ] vals = new Serializable[colCell.size()];
            int idx = 0;
            for (DataCell dataCell : colCell) {
                vals[idx++] = m_elementConverter.convert(dataCell, parameter);
            }
            return vals;
        }
        return null;
    }
}
