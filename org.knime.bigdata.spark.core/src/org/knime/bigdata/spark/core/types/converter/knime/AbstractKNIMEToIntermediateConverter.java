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
 *   Created on 24.04.2016 by koetter
 */
package org.knime.bigdata.spark.core.types.converter.knime;

import java.io.Serializable;
import java.util.Arrays;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;

import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;

/**
 * Abstract base class for all {@link KNIMEToIntermediateConverter} implementations.
 *
 * @author Tobias Koetter, KNIME.com
 */
public abstract class AbstractKNIMEToIntermediateConverter implements KNIMEToIntermediateConverter {

    private DataType m_knimeDataType;

    private IntermediateDataType m_intermediateDataType;

    private IntermediateDataType[] m_supportedIntermediateDataTypes;

    private String m_name;

    private String m_description;

    /**
     * Constructor for converter that support one KNIME {@link DataType} and one {@link IntermediateDataType}.
     *
     * @param name the user facing name of the converter
     * @param knimeType the KNIME {@link DataType} of the cell returned by the {@link #convert(Serializable)} method
     * @param intermediateType the {@link IntermediateDataType} returned by the {@link #convert(DataCell)} method
     */
    public AbstractKNIMEToIntermediateConverter(final String name, final DataType knimeType,
        final IntermediateDataType intermediateType) {

        this(name, defDesc(name), knimeType, intermediateType, new IntermediateDataType[]{intermediateType});
    }

    /**
     * Constructor for converter that support one KNIME {@link DataType} and one {@link IntermediateDataType}.
     *
     * @param name the user facing name of the converter
     * @param description the user facing description e.g. tool tip
     * @param knimeType the KNIME {@link DataType} of the cell returned by the {@link #convert(Serializable)} method
     * @param intermediateType the {@link IntermediateDataType} returned by the {@link #convert(DataCell)} method
     */
    public AbstractKNIMEToIntermediateConverter(final String name, final String description, final DataType knimeType,
        final IntermediateDataType intermediateType) {

        this(name, description, knimeType, intermediateType, new IntermediateDataType[]{intermediateType});
    }

    /**
     * Constructor for converter that support one KNIME {@link DataType} and multiple {@link IntermediateDataType}s.
     *
     * @param name the user facing name of the converter
     * @param knimeType the KNIME {@link DataType} of the cell returned by the {@link #convert(Serializable)} method
     * @param intermediateType the {@link IntermediateDataType} returned by the {@link #convert(DataCell)} method
     * @param supportedIntermediateTypes the supported {@link IntermediateDataType}s
     */
    public AbstractKNIMEToIntermediateConverter(final String name, final DataType knimeType,
        final IntermediateDataType intermediateType, final IntermediateDataType[] supportedIntermediateTypes) {

        this(name, defDesc(name), knimeType, intermediateType, supportedIntermediateTypes);
    }

    /**
     * Constructor for converter that support one KNIME {@link DataType} and multiple {@link IntermediateDataType}s.
     *
     * @param name the user facing name of the converter
     * @param description the user facing description e.g. tool tip
     * @param knimeType the KNIME {@link DataType} of the cell returned by the {@link #convert(Serializable)} method
     * @param intermediateType the {@link IntermediateDataType} returned by the {@link #convert(DataCell)} method
     * @param supportedIntermediateTypes the supported {@link IntermediateDataType}s
     */
    public AbstractKNIMEToIntermediateConverter(final String name, final String description, final DataType knimeType,
        final IntermediateDataType intermediateType, final IntermediateDataType[] supportedIntermediateTypes) {

        if (supportedIntermediateTypes == null) {
            throw new IllegalArgumentException("supportedIntermediateTypes must not be null");
        }

        if (Arrays.asList(supportedIntermediateTypes).indexOf(intermediateType) == -1) {
            throw new IllegalArgumentException("intermediateType is not in list of asupportedIntermediateTypes");
        }

        m_name = name;
        m_description = description;
        m_knimeDataType = knimeType;
        m_intermediateDataType = intermediateType;
        m_supportedIntermediateDataTypes = supportedIntermediateTypes;
    }

    /**
     * @param name the name of the converter
     * @return the default description for the converter based on the name
     */
    public static String defDesc(final String name) {
        return "Converts " + name.toLowerCase() + " values";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return m_name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return m_description;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntermediateDataType getIntermediateDataType() {
        return m_intermediateDataType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntermediateDataType[] getSupportedIntermediateDataTypes() {
        return m_supportedIntermediateDataTypes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getKNIMEDataType() {
        return m_knimeDataType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Serializable convert(final DataCell cell) {
        if (cell.isMissing()) {
            return null;
        }
        return convertNoneMissingCell(cell);
    }

    /**
     * @param cell the {@link DataCell}. Can not be a missing {@link DataCell}!
     * @return the {@link Serializable} for the not missing {@link DataCell}
     */
    protected abstract Serializable convertNoneMissingCell(DataCell cell);

    /**
     * @param cell the incompatible cell
     * @return the exception
     */
    protected IllegalArgumentException incompatibleCellException(final DataCell cell) {
        return new IllegalArgumentException("Cell " + cell + " not compatible with " + getClass().getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataCell convert(final Serializable intermediateTypeObject) {
        if (intermediateTypeObject == null) {
            return DataType.getMissingCell();
        }
        return convertNotNullSerializable(intermediateTypeObject);
    }

    /**
     * @param intermediateTypeObject not <code>null</code> {@link Serializable}
     * @return the {@link DataCell} representation of the not <code>null</code> {@link Serializable}
     */
    protected abstract DataCell convertNotNullSerializable(Serializable intermediateTypeObject);

    /**
     * @param intermediateTypeObject the incompatible intermediateTypeObject
     * @return the exception
     */
    protected IllegalArgumentException incompatibleSerializableException(final Serializable intermediateTypeObject) {
        return new IllegalArgumentException(
            "IntermediateTypeObject " + intermediateTypeObject + " not compatible with " + getClass().getName());
    }
}
