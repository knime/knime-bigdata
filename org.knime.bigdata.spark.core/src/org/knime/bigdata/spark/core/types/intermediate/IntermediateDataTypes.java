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
package org.knime.bigdata.spark.core.types.intermediate;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Provides access to all predefined {@link IntermediateDataType} singletons as well as factory methods
 * for complex types such as array or map types.
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public final class IntermediateDataTypes {

    /**Binary type.*/
    public static final IntermediateDataType BINARY = new IntermediateDataType("binary");
    /**Boolean type.*/
    public static final IntermediateDataType BOOLEAN = new IntermediateDataType("boolean");
    /**Byte type.*/
    public static final IntermediateDataType BYTE = new IntermediateDataType("byte");
    /**Calendar type.*/
    public static final IntermediateDataType CALENDAR_INTERVAL = new IntermediateDataType("calendarInterval");
    /**Date type.*/
    public static final IntermediateDataType DATE = new IntermediateDataType("date");
    /**Double type.*/
    public static final IntermediateDataType DOUBLE = new IntermediateDataType("double");
    /**Float type.*/
    public static final IntermediateDataType FLOAT = new IntermediateDataType("float");
    /**Integer type.*/
    public static final IntermediateDataType INTEGER = new IntermediateDataType("integer");
    /**Long type.*/
    public static final IntermediateDataType LONG = new IntermediateDataType("long");
    /**Null type.*/
    public static final IntermediateDataType NULL = new IntermediateDataType("null");
    /**Short type.*/
    public static final IntermediateDataType SHORT = new IntermediateDataType("short");
    /**String type.*/
    public static final IntermediateDataType STRING = new IntermediateDataType("string");
    /**Timestamp type.*/
    public static final IntermediateDataType TIMESTAMP = new IntermediateDataType("timestamp");
    /**
     * Any type that matches any intermediate data type. Only for use by default converters.
     * */
    public static final IntermediateDataType ANY = new IntermediateDataType("*");

    /**
     * @param baseType the base {@link IntermediateDataType}
     * @return the {@link IntermediateArrayDataType} for the given base type
     */
    public static IntermediateArrayDataType createArrayType(final IntermediateDataType baseType) {
        return new IntermediateArrayDataType(baseType);
    }

    /**
     * @param keyType the {@link IntermediateDataType} of the key
     * @param valueType the {@link IntermediateDataType} of the value
     * @return the {@link IntermediateMapDataType}
     */
    public static IntermediateMapDataType createMapType(final IntermediateDataType keyType,
        final IntermediateDataType valueType) {
        return new IntermediateMapDataType(keyType, valueType);
    }

    /**
     *
     */
    private IntermediateDataTypes() {
        // prevent object creation
    }
}
