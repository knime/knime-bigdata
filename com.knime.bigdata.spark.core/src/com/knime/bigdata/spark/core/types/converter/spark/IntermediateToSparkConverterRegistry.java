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
package com.knime.bigdata.spark.core.types.converter.spark;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import com.knime.bigdata.spark.core.version.SparkProviderRegistry;
import com.knime.bigdata.spark.core.version.SparkProviderWithElements;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class IntermediateToSparkConverterRegistry extends SparkProviderRegistry<SparkProviderWithElements<IntermediateToSparkConverter<?>>> {

    /**The id of the converter extension point.*/
    public static final String EXT_POINT_ID = "com.knime.bigdata.spark.core.IntermediateToSparkConverterProvider";

    private static volatile IntermediateToSparkConverterRegistry instance;

    private final Map<SparkVersion, Map<IntermediateDataType, IntermediateToSparkConverter<?>>> m_converters =
            new LinkedHashMap<>();

    private IntermediateToSparkConverterRegistry() {}

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static IntermediateToSparkConverterRegistry getInstance() {
        if (instance == null) {
            synchronized (IntermediateToSparkConverterRegistry.class) {
                if (instance == null) {
                    instance = new IntermediateToSparkConverterRegistry();
                    instance.registerExtensions(EXT_POINT_ID);
                }
            }
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void addProvider(final SparkProviderWithElements<IntermediateToSparkConverter<?>> provider) {
        for(final SparkVersion sparkVersion : provider.getSupportedSparkVersions()) {

            final Map<IntermediateDataType, IntermediateToSparkConverter<?>> converterMap =
                    m_converters.getOrDefault(sparkVersion,
                        new HashMap<IntermediateDataType, IntermediateToSparkConverter<?>>());

            final Map<IntermediateDataType, IntermediateToSparkConverter<?>> validatedNewConverters = new HashMap<>();

            for (final IntermediateToSparkConverter<?> newConverter : provider.get()) {
                if (converterMap.containsKey(newConverter.getIntermediateDataType())) {
                    throw new IllegalStateException(String.format(
                        "There is already a type converter (Intermediate <-> Spark) for intermediate type %s and Spark version %s. Ignoring %s.",
                        newConverter.getIntermediateDataType().getTypeId(),
                        sparkVersion.getLabel(),
                        newConverter.getClass().getName()));
                } else if (newConverter.getIntermediateDataType() == IntermediateDataTypes.ANY && !(newConverter instanceof AnyIntermediateToSparkConverter)) {
                    throw new IllegalStateException(String.format(
                        "A type converter (Intermediate <-> Spark) for intermediate type %s must implement the %s interface",
                        newConverter.getIntermediateDataType().getTypeId(),
                        AnyIntermediateToSparkConverter.class.getCanonicalName()));
                } else {
                    validatedNewConverters.put(newConverter.getIntermediateDataType(), newConverter);
                }
            }

            // commit all validated type converters to the official converter map
            converterMap.putAll(validatedNewConverters);

            m_converters.put(sparkVersion, converterMap);
        }
    }

    /**
     * Returns all registered type converters for the given Spark version.
     *
     * @param sparkVersion the {@link SparkVersion}
     * @return all registered type converters as a list
     */
    public static List<IntermediateToSparkConverter<?>> getConverters(final SparkVersion sparkVersion) {
        return new LinkedList<>(getInstance().m_converters.get(sparkVersion).values());
    }
}
