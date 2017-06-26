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

import com.knime.bigdata.spark.core.types.TypeConverter;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateArrayDataType;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
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

    private KNIMEToIntermediateConverterRegistry() {
        //avoid object creation
        addConverter(BooleanType.INSTANCE);
        addConverter(DateAndTimeType.INSTANCE);
        addConverter(DoubleType.INSTANCE);
        addConverter(IntegerType.INSTANCE);
        addConverter(LocalDateType.INSTANCE);
        addConverter(LocalDateTimeType.INSTANCE);
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
     * @param typeConverter the {@link KNIMEToIntermediateConverter} to register
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
     * @param type the {@link DataType} to get the converter for
     * @return the {@link KNIMEToIntermediateConverter} to use or the default converter
     * @see #getDefaultConverter()
     */
    public static KNIMEToIntermediateConverter get(final DataType type) {
        //we have to handle collection types special
        if (type.isCollectionType()) {
            final DataType elementType = type.getCollectionElementType();
            final KNIMEToIntermediateConverter converter = get(elementType);
            return new CollectionType(type, converter);
        }
        final KNIMEToIntermediateConverter converter = getInstance().m_knime2Intermediate.get(type);
        return (converter != null) ? converter : DEFAULT_CONVERTER;
    }

    /**
     * @param type the {@link IntermediateDataType} to get the converter for
     * @return the {@link KNIMEToIntermediateConverter} to use or the default converter
     * @see #getDefaultConverter()
     * @deprecated This implementation returns the first available converter.
     */
    @Deprecated
    private static KNIMEToIntermediateConverter get(final IntermediateDataType type) {
        return getConverterList(getInstance().m_intermediate2Knime, type).iterator().next();
    }

    /**
     * @param type the {@link IntermediateDataType} to get the converter for
     * @return the {@link KNIMEToIntermediateConverter} to use or the default converter
     * @see #getDefaultConverter()
     */
    @Deprecated
    private static Collection<KNIMEToIntermediateConverter> getAll(final IntermediateDataType type) {
        return getConverterList(getInstance().m_intermediate2Knime, type);
    }

    @Deprecated
    private static <K extends IntermediateDataType> Collection<KNIMEToIntermediateConverter>
        getConverterList(final Map<K, Collection<KNIMEToIntermediateConverter>> map, final K key) {
        if (key instanceof IntermediateArrayDataType) {
            final IntermediateArrayDataType arrayType = (IntermediateArrayDataType) key;
            final IntermediateDataType baseType = arrayType.getBaseType();
            final Collection<KNIMEToIntermediateConverter> converters = getAll(baseType);
            final Collection<KNIMEToIntermediateConverter> arrayConverter = new ArrayList<>(converters.size());
            for (KNIMEToIntermediateConverter converter : converters) {
                arrayConverter.add(new CollectionType(null, converter));
            }
            return arrayConverter;
        }
        final Collection<KNIMEToIntermediateConverter> converter = map.get(key);
        return converter != null ? converter : getDefaultConverterCollection();
    }

    private static Collection<KNIMEToIntermediateConverter> getDefaultConverterCollection() {
        return DEFAULT_CONVERTER_LIST;
    }

    /**
     * @return the default converter to use for all unknown type
     */
    public static TypeConverter getDefaultConverter() {
        return DEFAULT_CONVERTER;
    }

    /**
     * @param spec {@link DataTableSpec} to get the converter for
     * @return {@link KNIMEToIntermediateConverter} array with the appropriate converter for each data column of the
     *         input spec in the same order as the input columns
     */
    public static KNIMEToIntermediateConverter[] getConverter(final DataTableSpec spec) {
        KNIMEToIntermediateConverter[] converter = new KNIMEToIntermediateConverter[spec.getNumColumns()];
        int idx = 0;
        for (DataColumnSpec colSpec : spec) {
            converter[idx++] = get(colSpec.getType());
        }
        return converter;
    }

    /**
     * This converts the given {@link IntermediateSpec} into a {@link DataTableSpec} by retrieving the
     * first {@link KNIMEToIntermediateConverter} for each field of the {@link IntermediateSpec} and then using the
     * KNIME {@link DataType} of the returned converter. Use this with caution, as for {@link IntermediateDataType}s
     * that are the target type of multiple converters (e.g. {@link IntermediateDataTypes#BINARY}) this may not give
     * the result you want.
     *
     * @param intermediateSpec
     * @return a KNIME {@link DataTableSpec} with converted types.
     * @deprecated This implementation use the first available converter.
     */
    @Deprecated
    public static DataTableSpec convertSpec(final IntermediateSpec intermediateSpec) {
        final List<DataColumnSpec> knimeColumns = new LinkedList<>();

        final DataColumnSpecCreator specCreator = new DataColumnSpecCreator("foo", StringCell.TYPE);

        for (IntermediateField intermediateField : intermediateSpec.getFields()) {
            specCreator.setName(intermediateField.getName());
            specCreator.setType(get(intermediateField.getType()).getKNIMEDataType());
            knimeColumns.add(specCreator.createSpec());
        }
        return new DataTableSpec(knimeColumns.toArray(new DataColumnSpec[0]));
    }
}
