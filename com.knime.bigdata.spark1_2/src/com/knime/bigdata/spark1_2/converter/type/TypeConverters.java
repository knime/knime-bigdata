/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on Apr 26, 2016 by bjoern
 */
package com.knime.bigdata.spark1_2.converter.type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.api.java.ArrayType;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.types.converter.spark.AnyIntermediateToSparkConverter;
import com.knime.bigdata.spark.core.types.converter.spark.IntermediateArrayToSparkConverter;
import com.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverter;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateArrayDataType;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark1_2.converter.type.SerializableTypeProxies.ArrayTypeProxy;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class TypeConverters {

    private static final Map<IntermediateDataType, IntermediateToSparkConverter<DataType>> toSparkConverterMap =
        new HashMap<>();

    private static final Map<DataType, IntermediateToSparkConverter<DataType>> toIntermediateConverterMap =
        new HashMap<>();

    public static synchronized void ensureConvertersInitialized(final Collection<IntermediateToSparkConverter<DataType>> converters) {
        if (toSparkConverterMap.isEmpty()) {
            for (IntermediateToSparkConverter<DataType> converter : converters) {
                toSparkConverterMap.put(converter.getIntermediateDataType(), converter);

                // do not put the default converter in toIntermediateConverterMap
                // because it will overwrite one of the existing converters
                if (converter.getIntermediateDataType() != IntermediateDataTypes.ANY) {
                    toIntermediateConverterMap.put(converter.getSparkDataType(), converter);
                }
            }

            if (!toSparkConverterMap.containsKey(IntermediateDataTypes.ANY)) {
                // this is a bug, so we use a runtime exception
                throw new IllegalArgumentException(String.format("No fallback type converter for intermediate type %s was provided", IntermediateDataTypes.ANY.getTypeId()));
            }
        }
    }

    public IntermediateToSparkConverter<? extends DataType> getDefaultConverter() {
        return getConverter(IntermediateDataTypes.ANY);
    }

    public static IntermediateToSparkConverter<? extends DataType> getConverter(
        final IntermediateDataType intermediateType) {
        if (intermediateType instanceof IntermediateArrayDataType) {
            final IntermediateArrayDataType arrayType = (IntermediateArrayDataType)intermediateType;
            final IntermediateDataType elementType = arrayType.getBaseType();
            final IntermediateToSparkConverter<? extends DataType> elementConverter =
                    getConverter(elementType);
            final ArrayTypeProxy arrayTypeProxy =
                    new SerializableTypeProxies.ArrayTypeProxy(elementConverter.getSerializableDataType());
            return new IntermediateArrayToSparkConverter<>(elementConverter, arrayTypeProxy);
        }
        return toSparkConverterMap.get(intermediateType);
    }

    public static IntermediateToSparkConverter<? extends DataType> getConverter(final DataType sparkDataType) {
        if (sparkDataType instanceof ArrayType) {
            final ArrayType arrayType = (ArrayType)sparkDataType;
            final DataType elementType = arrayType.getElementType();
            final IntermediateToSparkConverter<? extends DataType> elementConverter =
                    getConverter(elementType);
            final ArrayTypeProxy arrayTypeProxy =
                    new SerializableTypeProxies.ArrayTypeProxy(elementConverter.getSerializableDataType());
            return new IntermediateArrayToSparkConverter<>(elementConverter, arrayTypeProxy);
        }
        return toIntermediateConverterMap.get(sparkDataType);
    }

    @SuppressWarnings("unchecked")
    public static IntermediateToSparkConverter<DataType>[] getConverters(final IntermediateSpec spec) {
        IntermediateToSparkConverter<?>[] converters = new IntermediateToSparkConverter<?>[spec.getNoOfFields()];
        int idx = 0;
        for (IntermediateField field : spec.getFields()) {
            converters[idx++] = getConverter(field.getType());
        }
        return (IntermediateToSparkConverter<DataType>[])converters;
    }

    @SuppressWarnings("unchecked")
    public static IntermediateToSparkConverter<DataType>[] getConverters(final StructType spec) {
        IntermediateToSparkConverter<?>[] converters = new IntermediateToSparkConverter<?>[spec.getFields().length];

        int idx = 0;
        for (StructField field : spec.getFields()) {
            converters[idx++] = getConverter(field.getDataType());
        }
        return (IntermediateToSparkConverter<DataType>[])converters;
    }

    /**
     * Converts the given {@link StructType} spec to an {@link IntermediateSpec}
     * by reverse-lookup of the {@link IntermediateToSparkConverter} using the Spark
     * data type.
     *
     * @param specToConvert
     * @return an matching {@link IntermediateSpec}
     */
    public static IntermediateSpec convertSpec(final StructType specToConvert) {
        List<IntermediateField> convertedFields = new LinkedList<>();

        for (int i = 0; i < specToConvert.getFields().length; i++) {
            StructField fieldToConvert = specToConvert.getFields()[i];

            IntermediateToSparkConverter<?> converter = getConverter(fieldToConvert.getDataType());
            if (converter != null) {
                convertedFields.add(new IntermediateField(fieldToConvert.getName(),
                    converter.getIntermediateDataType(),
                    fieldToConvert.isNullable()));
            } else {
                converter = getConverter(IntermediateDataTypes.ANY);
                convertedFields.add(new IntermediateField(fieldToConvert.getName(),
                    ((AnyIntermediateToSparkConverter<?>) converter).getActualIntermediateType(),
                    fieldToConvert.isNullable()));
            }
        }

        return new IntermediateSpec(convertedFields.toArray(new IntermediateField[0]));
    }

    /**
     * @param specToConvert the {@link IntermediateSpec} to convert into a {@link StructType}
     * @return the {@link StructType} representation for the given {@link IntermediateSpec}
     */
    public static StructType convertSpec(final IntermediateSpec specToConvert) {
        final List<StructField> structFields = new ArrayList<>(specToConvert.getNoOfFields());
        for (IntermediateField field : specToConvert) {
            String name = field.getName();
            IntermediateDataType type = field.getType();
            final IntermediateToSparkConverter<? extends DataType> converter = getConverter(type);
            structFields.add(DataType.createStructField(name, converter.getSparkDataType(), true));
        }
        return DataType.createStructType(structFields);
    }
}
