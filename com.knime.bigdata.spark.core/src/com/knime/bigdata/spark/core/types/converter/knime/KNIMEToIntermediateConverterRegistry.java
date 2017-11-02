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
 *   Created on 05.07.2015 by koetter
 */
package com.knime.bigdata.spark.core.types.converter.knime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExtensionPoint;
import org.eclipse.core.runtime.IExtensionRegistry;
import org.eclipse.core.runtime.Platform;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.Version;

import com.knime.bigdata.spark.core.types.intermediate.IntermediateArrayDataType;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Registry class by which to obtain the {@link KNIMEToIntermediateConverter}s registered at the
 * KNIMEToIntermediateConverter extension point.
 *
 * @author Tobias Koetter, KNIME GmbH
 */
public class KNIMEToIntermediateConverterRegistry {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(KNIMEToIntermediateConverterRegistry.class);

    /** The id of the converter extension point. */
    public static final String EXT_POINT_ID = "com.knime.bigdata.spark.core.KNIMEToIntermediateConverter";

    /** The attribute of the converter extension point. */
    public static final String EXT_POINT_ATTR_DF = "ProviderClass";

    private static volatile KNIMEToIntermediateConverterRegistry instance;

    private static final KNIMEToIntermediateConverter DEFAULT_CONVERTER = StringType.INSTANCE;

    private static final Collection<KNIMEToIntermediateConverter> DEFAULT_CONVERTER_LIST =
        Arrays.asList(new KNIMEToIntermediateConverter[]{DEFAULT_CONVERTER});

    private final Map<DataType, KNIMEToIntermediateConverter> m_knime2Intermediate = new HashMap<>();

    private final Map<IntermediateDataType, Collection<KNIMEToIntermediateConverter>> m_intermediate2Knime =
        new HashMap<>();

    @SuppressWarnings("deprecation")
    private KNIMEToIntermediateConverterRegistry() {
        //avoid object creation
        addConverter(BooleanType.INSTANCE);
        addConverter(LocalDateType.INSTANCE);
        addConverter(LocalDateTimeType.INSTANCE);
        addConverter(DateAndTimeType.INSTANCE);
        addConverter(DoubleType.INSTANCE);
        addConverter(IntegerType.INSTANCE);
        addConverter(LongType.INSTANCE);
        addConverter(StringType.INSTANCE);
        //register all extension point implementations
        registerExtensionPointImplementations();
    }

    /**
     * Returns the only instance of this class.
     *
     * @return the only instance
     */
    public static KNIMEToIntermediateConverterRegistry getInstance() {
        if (instance == null) {
            synchronized (KNIMEToIntermediateConverterRegistry.class) {
                if (instance == null) {
                    instance = new KNIMEToIntermediateConverterRegistry();
                }
            }
        }
        return instance;
    }

    /**
     * Registers all extension point implementations.
     */
    private void registerExtensionPointImplementations() {
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
                    final KNIMEToIntermediateConverter typeConverter =
                        (KNIMEToIntermediateConverter)elem.createExecutableExtension(EXT_POINT_ATTR_DF);
                    addConverter(typeConverter);
                } catch (final Throwable t) {
                    LOGGER.error("Problems during initialization of " + KNIMEToIntermediateConverter.class.getName()
                        + " (with id '" + converter + "'.)", t);
                    if (decl != null) {
                        LOGGER.error("Extension " + decl + " ignored.", t);
                    }
                }
            }
        } catch (final Exception e) {
            LOGGER.error("Exception while registering " + KNIMEToIntermediateConverter.class.getName() + "extensions",
                e);
        }
    }

    /**
     * Adds a type converter to the internal maps.
     */
    private void addConverter(final KNIMEToIntermediateConverter typeConverter) {

        final DataType knimeType = typeConverter.getKNIMEDataType();
        if (m_knime2Intermediate.containsKey(knimeType)) {
            throw new IllegalStateException(String.format(
                "KNIME data type %s is already mapped to intermediate type %s. Ignoring type converter %s.",
                knimeType.getName(), m_knime2Intermediate.get(knimeType).getIntermediateDataType().getTypeId(),
                typeConverter.getClass().getName()));
        } else {
            m_knime2Intermediate.put(knimeType, typeConverter);
        }

        for (IntermediateDataType intermediateType : typeConverter.getSupportedIntermediateDataTypes()) {
            Collection<KNIMEToIntermediateConverter> converters = m_intermediate2Knime.get(intermediateType);
            if (converters == null) {
                converters = new LinkedList<>();
                m_intermediate2Knime.put(intermediateType, converters);
            }
            converters.add(typeConverter);
        }
    }

    /**
     * Returns a type converter that (1) converts the given KNIME {@link DataType} into an intermediate data type and
     * (2) supports the given version of KNIME Spark Executor, i.e. one which is supposed to be used by nodes that were
     * created with the given version. This "version constraint" ensures that old workflows with Spark nodes continue to
     * produce exactly the same results even in future versions of KNIME Spark Executor. If no matching type converter
     * could be found a default type converted will be returned.
     *
     * @param type The KNIME {@link DataType} to get the converter for.
     * @param knimeSparkExecutorVersion The version of KNIME Spark Executor that must be supported by the converter.
     * @return A {@link KNIMEToIntermediateConverter} to use for the given KNIME type and version.
     * @see #getDefaultConverter()
     * @since 2.1.0
     */
    public static KNIMEToIntermediateConverter get(final DataType type, final Version knimeSparkExecutorVersion) {
        //we have to handle collection types special
        if (type.isCollectionType()) {
            final DataType elementType = type.getCollectionElementType();
            final KNIMEToIntermediateConverter converter = get(elementType, knimeSparkExecutorVersion);
            return new CollectionType(type, converter);
        }

        final KNIMEToIntermediateConverter converter = getInstance().m_knime2Intermediate.get(type);

        if (converter != null && converter.supportsVersion(knimeSparkExecutorVersion)) {
            return converter;
        } else {
            return DEFAULT_CONVERTER;
        }
    }

    /**
     * Returns a type converter that (1) converts the given {@link IntermediateDataType} into a KNIME {@link DataType}
     * and (2) supports the given version of KNIME Spark Executor, i.e. one which is supposed to be used by nodes that were
     * created with the given version. This "version constraint" ensures that old workflows with Spark nodes continue to
     * produce exactly the same results even in future versions of KNIME Spark Executor. If no matching type converter
     * could be found a default type converter will be returned.
     *
     * <p>
     * Note there may be multiple type converters that match. This method returns the "first" one, so this may change
     * between versions of KNIME Spark Executor (which is why this method is deprecated).
     * </p>
     *
     * @param type The {@link IntermediateDataType} to get the converter for.
     * @param knimeSparkExecutorVersion The version of KNIME Spark Executor that must be supported by the converter.
     * @return the {@link KNIMEToIntermediateConverter} to use or the default converter
     * @see #getDefaultConverter()
     * @deprecated This implementation returns the first available converter, which may change between versions of KNIME
     *             Spark Executor. New code should use {@link #getAll(IntermediateDataType, Version)} instead.
     */
    @Deprecated
    public static KNIMEToIntermediateConverter get(final IntermediateDataType type,
        final Version knimeSparkExecutorVersion) {
        return getAll(type, knimeSparkExecutorVersion).iterator().next();
    }


    /**
     * Returns a list of type converters that (1) convert the given {@link IntermediateDataType} into a KNIME
     * {@link DataType} and (2) support the given version of KNIME Spark Executor, i.e. which are supposed to be used by
     * nodes that were created with the given version. This "version constraint" ensures that old workflows with Spark
     * nodes continue to produce exactly the same results even in future versions of KNIME Spark Executor. If no
     * matching type converter could be found, then default type converters will be returned.
     *
     * @param type The {@link IntermediateDataType} to get the converters for.
     * @param knimeSparkExecutorVersion The version of KNIME Spark Executor that must be supported by the converters.
     * @return a list of matching {@link KNIMEToIntermediateConverter}, or default type converters.
     * @since 2.1.0
     * @see #getDefaultConverter()
     */
    public static Collection<KNIMEToIntermediateConverter> getAll(final IntermediateDataType type,
        final Version knimeSparkExecutorVersion) {
        if (type instanceof IntermediateArrayDataType) {
            final IntermediateArrayDataType arrayType = (IntermediateArrayDataType)type;
            final IntermediateDataType baseType = arrayType.getBaseType();
            final Collection<KNIMEToIntermediateConverter> baseTypeConverters =
                getAll(baseType, knimeSparkExecutorVersion);

            final Collection<KNIMEToIntermediateConverter> arrayConverters = new ArrayList<>(baseTypeConverters.size());
            for (KNIMEToIntermediateConverter converter : baseTypeConverters) {
                arrayConverters.add(new CollectionType(null, converter));
            }
            return arrayConverters;
        }

        final Collection<KNIMEToIntermediateConverter> candidateConverters =
            getInstance().m_intermediate2Knime.get(type);
        final Collection<KNIMEToIntermediateConverter> toReturn;
        if (candidateConverters == null) {
            toReturn = filterByKNIMESparkExecutorVersion(getDefaultConverterCollection(), knimeSparkExecutorVersion);
        } else {
            toReturn = filterByKNIMESparkExecutorVersion(candidateConverters, knimeSparkExecutorVersion);
        }
        return toReturn;
    }

    private static Collection<KNIMEToIntermediateConverter> filterByKNIMESparkExecutorVersion(
        final Collection<KNIMEToIntermediateConverter> candidates, final Version knimeSparkExecutorVersion) {

        final List<KNIMEToIntermediateConverter> toReturn = new LinkedList<>();
        for (KNIMEToIntermediateConverter candidate : candidates) {
            if (candidate.supportsVersion(knimeSparkExecutorVersion)) {
                toReturn.add(candidate);
            }
        }

        return toReturn;
    }

    private static Collection<KNIMEToIntermediateConverter> getDefaultConverterCollection() {
        return DEFAULT_CONVERTER_LIST;
    }

    /**
     * Return the default {@link KNIMEToIntermediateConverter} that converts any KNIME {@link DataType} into and
     * intermediate type and any intermediate type into a KNIME {@link DataType}. Currently this this is
     * {@link StringType}.
     *
     * @return the default converter
     */
    public static KNIMEToIntermediateConverter getDefaultConverter() {
        return DEFAULT_CONVERTER;
    }

    /**
     * Returns the type converters to convert the given KNIME {@link DataTableSpec} into an {@link IntermediateSpec}.
     * See {@link #get(DataType, Version)} to see how the converters are chosen.
     *
     * @param spec The KNIME {@link DataTableSpec} to get the converters for.
     * @param knimeSparkExecutorVersion The version of KNIME Spark Executor that must be supported by the converters.
     * @return an array of {@link KNIMEToIntermediateConverter}s
     * @see #get(DataType, Version)
     * @since 2.1.0
     */
    public static KNIMEToIntermediateConverter[] getConverters(final DataTableSpec spec, final Version knimeSparkExecutorVersion) {
        KNIMEToIntermediateConverter[] converter = new KNIMEToIntermediateConverter[spec.getNumColumns()];
        int idx = 0;
        for (DataColumnSpec colSpec : spec) {
            converter[idx++] = get(colSpec.getType(), knimeSparkExecutorVersion);
        }
        return converter;
    }

    /**
     * Converts the given {@link IntermediateSpec} into a KNIME {@link DataTableSpec}.
     *
     * <p>
     * For each column this method picks a converter that (1) converts the respective {@link IntermediateDataType} into
     * a KNIME {@link DataType} and (2) supports the given version of KNIME Spark Executor, i.e. one which is supposed
     * to be used by nodes that were created with the given version. This "version constraint" ensures that old
     * workflows with Spark nodes continue to produce exactly the same results even in future versions of KNIME Spark
     * Executor. If no matching type converter could be found, then default type converter is used. Note there may be
     * multiple type converters that match. This method returns the "first" one, so it may use converters that produce
     * undesired KNIME types.
     * </p>
     *
     * @param intermediateSpec
     * @param knimeSparkExecutorVersion The version of KNIME Spark Executor tp be used while selecting type converters.
     * @return a KNIME {@link DataTableSpec} with converted types.
     * @deprecated This method may produce a spec with undesired KNIME data types. New code should use
     *             {@link #getAll(IntermediateDataType, Version)} and pick the converted that produces the desired KNIME
     *             type.
     */
    @Deprecated
    public static DataTableSpec convertSpec(final IntermediateSpec intermediateSpec, final Version knimeSparkExecutorVersion) {
        final List<DataColumnSpec> knimeColumns = new LinkedList<>();

        final DataColumnSpecCreator specCreator = new DataColumnSpecCreator("foo", StringCell.TYPE);

        for (IntermediateField intermediateField : intermediateSpec.getFields()) {
            specCreator.setName(intermediateField.getName());
            specCreator.setType(get(intermediateField.getType(), knimeSparkExecutorVersion).getKNIMEDataType());
            knimeColumns.add(specCreator.createSpec());
        }
        return new DataTableSpec(knimeColumns.toArray(new DataColumnSpec[0]));
    }
}
