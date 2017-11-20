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
 *   Created on Apr 19, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.types.converter.knime;

import java.io.Serializable;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;

import org.knime.bigdata.spark.core.types.TypeConverter;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateField;

/**
 * Interface for all {@link TypeConverter}s, that convert from KNIME {@link DataType}s to {@link IntermediateDataType}s.
 * Each implementation of this interface converts between a <em>single</em> KNIME {@link DataType}s and
 * <em>possibly</em> multiple {@link IntermediateDataType}s. Implementation must honor the following contract:
 * <ul>
 * <li>Invoking {@link #convert(DataCell)} always returns a value with type {@link #getIntermediateDataType()} (this is
 * the so-called <em>preferred type</em>)</li>
 * <li>Invoking {@link #convert(Serializable)} always returns a {@link DataCell} with type {@link #getKNIMEDataType()}
 * </li>
 * </ul>
 *
 * <p>
 * Implementations can be registered via the <em>KNIMEToIntermediateConverter</em> extension point. It is not possible
 * to register two implementations with the same KNIME {@link DataType} and overlapping
 * </p>
 *
 * @author Bjoern Lohrmann, KNIME.com
 * @see KNIMEToIntermediateConverterRegistry
 */
public interface KNIMEToIntermediateConverter extends TypeConverter {

    /**
     * @return the preferred {@link IntermediateField} type this converter converts the supported KNIME type to
     */
    public IntermediateDataType getIntermediateDataType();

    /**
     * @return the supported {@link IntermediateDataType}s
     */
    public IntermediateDataType[] getSupportedIntermediateDataTypes();

    /**
     * @return the KNIME {@link DataType} this converter converts the supported intermediate types to
     */
    public DataType getKNIMEDataType();

    /**
     * Converts a value from one of the supported intermediate type domains (see
     * {@link #getSupportedIntermediateDataTypes()}) to value from the KNIME data type.
     *
     * @param intermediateTypeObject a value from one of the intermediate type domains. May be null.
     * @return corresponding KNIME {@link DataCell} of type {@link #getKNIMEDataType()} or
     *         {@link DataType#getMissingCell()} if the object is <code>null</code>
     */
    public DataCell convert(Serializable intermediateTypeObject);

    /**
     * Converts a value from the KNIME type domain (see {@link #getKNIMEDataType()}) to value from the intermediate type
     * domain (see {@link #getIntermediateDataType()}).
     *
     * @param cell A KNIME {@link DataCell} to convert.
     * @return the corresponding value from the intermediate type domain (see {@link #getIntermediateDataType()})
     */
    public Serializable convert(DataCell cell);
}
