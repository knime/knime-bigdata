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
 *   Created on 05.07.2015 by koetter
 */
package com.knime.bigdata.spark.util.converter;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExtensionPoint;
import org.eclipse.core.runtime.IExtensionRegistry;
import org.eclipse.core.runtime.Platform;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.NodeLogger;

import com.knime.bigdata.spark.util.converter.types.BooleanType;
import com.knime.bigdata.spark.util.converter.types.DateAndTimeType;
import com.knime.bigdata.spark.util.converter.types.DoubleType;
import com.knime.bigdata.spark.util.converter.types.IntegerType;
import com.knime.bigdata.spark.util.converter.types.LongType;
import com.knime.bigdata.spark.util.converter.types.StringType;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkTypeRegistry {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkTypeRegistry.class);

    /**The id of the converter extension point.*/
    public static final String EXT_POINT_ID =
            "com.knime.bigdata.Spark.SparkTypeConverter";

    /**The attribute of the converter extension point.*/
    public static final String EXT_POINT_ATTR_DF = "SparkTypeConverter";

    private static volatile SparkTypeRegistry instance;

    private final Map<DataType, SparkTypeConverter<?, ?>> m_knime = new HashMap<>();
    private final Map<org.apache.spark.sql.api.java.DataType, SparkTypeConverter<?, ?>> m_spark = new HashMap<>();

    private SparkTypeRegistry() {
        //avoid object creation
        addConverter(BooleanType.getInstance());
        addConverter(DateAndTimeType.getInstance());
        addConverter(DoubleType.getInstance());
        addConverter(IntegerType.getInstance());
        addConverter(LongType.getInstance());
        addConverter(StringType.getInstance());
        //register all extension point implementations
        registerExtensionPoints();
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static SparkTypeRegistry getInstance() {
        if (instance == null) {
            synchronized (SparkTypeRegistry.class) {
                if (instance == null) {
                    instance = new SparkTypeRegistry();
                }
            }
        }
        return instance;
    }


    /**
     * Registers all extension point implementations.
     */
    private void registerExtensionPoints() {
        try {
            final IExtensionRegistry registry = Platform.getExtensionRegistry();
            final IExtensionPoint point = registry.getExtensionPoint(EXT_POINT_ID);
            if (point == null) {
                LOGGER.error("Invalid extension point: " + EXT_POINT_ID);
                throw new IllegalStateException("ACTIVATION ERROR: --> Invalid extension point: " + EXT_POINT_ID);
            }
            for (final IConfigurationElement elem : point.getConfigurationElements()) {
                final String converter = elem.getAttribute(EXT_POINT_ATTR_DF);
                final String decl = elem.getDeclaringExtension().getUniqueIdentifier();

                if (converter == null || converter.isEmpty()) {
                    LOGGER.error("The extension '" + decl + "' doesn't provide the required attribute '"
                            + EXT_POINT_ATTR_DF + "'");
                    LOGGER.error("Extension " + decl + " ignored.");
                    continue;
                }
                try {
                    final SparkTypeConverter<?, ?> typeConverter =
                            (SparkTypeConverter<?, ?>)elem.createExecutableExtension(EXT_POINT_ATTR_DF);
                    addConverter(typeConverter);
                } catch (final Throwable t) {
                    LOGGER.error("Problems during initialization of Spark TypeConverter (with id '" + converter
                        + "'.)", t);
                    if (decl != null) {
                        LOGGER.error("Extension " + decl + " ignored.", t);
                    }
                }
            }
        } catch (final Exception e) {
            LOGGER.error("Exception while registering aggregation operator extensions", e);
        }
    }

    /**
     * @param converter the {@link SparkTypeConverter} to register
     */
    private void addConverter(final SparkTypeConverter<?, ?> converter) {
        final DataType[] knimeTypes = converter.getKNIMETypes();
        for (DataType knimeType : knimeTypes) {
            final SparkTypeConverter<?, ?> old = m_knime.put(knimeType, converter);
            if (old != null) {
                LOGGER.info("Replace default converter " + old.getClass().getName()
                    + " with " + converter.getClass().getName());
            }
        }
        final org.apache.spark.sql.api.java.DataType[] sparkSqlTypes = converter.getSparkSqlTypes();
        for (org.apache.spark.sql.api.java.DataType sparkSqlType : sparkSqlTypes) {
            final SparkTypeConverter<?, ?> old = m_spark.put(sparkSqlType, converter);
            if (old != null) {
                LOGGER.info("Replace default converter " + old.getClass().getName()
                    + " with " + converter.getClass().getName());
            }
        }
    }

    /**
     * @param type the {@link DataType} to get the converter for
     * @return the {@link SparkTypeConverter} to use or the default converter
     * @see #getDefaultConverter()
     */
    public static SparkTypeConverter<?, ?> get(final DataType type) {
        SparkTypeConverter<?, ?> converter = getInstance().m_knime.get(type);
        return converter != null ? converter : getDefaultConverter();
    }

    /**
     * @param type the {@link org.apache.spark.sql.api.java.DataType} to get the converter for
     * @return the {@link SparkTypeConverter} to use or the default converter
     * @see #getDefaultConverter()
     */
    public static SparkTypeConverter<?, ?> get(final org.apache.spark.sql.api.java.DataType type) {
        SparkTypeConverter<?, ?> converter = getInstance().m_spark.get(type);
        return converter != null ? converter : getDefaultConverter();
    }

    /**
     * @return the default converter to use for all unknown type
     */
    public static SparkTypeConverter<StringCell, String> getDefaultConverter() {
        return StringType.getInstance();
    }

    /**
     * @param spec {@link DataTableSpec} to get the converter for
     * @return {@link SparkTypeConverter} array with the appropriate converter for each data column of the
     * input spec int he same order as the input columns
     */
    public static SparkTypeConverter<?, ?>[] getConverter(final DataTableSpec spec) {
        SparkTypeConverter<?, ?>[] converter = new SparkTypeConverter<?, ?>[spec.getNumColumns()];
        int idx = 0;
        for (DataColumnSpec colSpec : spec) {
            converter[idx++] = get(colSpec.getType());
        }
        return converter;
    }
}
