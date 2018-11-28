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
package org.knime.bigdata.spark2_4.converter.type;

import org.knime.bigdata.spark.core.types.converter.spark.DefaultIntermediateToSparkConverter;
import org.knime.bigdata.spark.core.types.converter.spark.DefaultIntermediateToSparkConverterProvider;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import org.knime.bigdata.spark2_4.api.Spark_2_4_CompatibilityChecker;
import org.knime.bigdata.spark2_4.converter.type.SerializableTypeProxies.BinaryTypeProxy;
import org.knime.bigdata.spark2_4.converter.type.SerializableTypeProxies.BooleanTypeProxy;
import org.knime.bigdata.spark2_4.converter.type.SerializableTypeProxies.ByteTypeProxy;
import org.knime.bigdata.spark2_4.converter.type.SerializableTypeProxies.DateTypeProxy;
import org.knime.bigdata.spark2_4.converter.type.SerializableTypeProxies.DoubleTypeProxy;
import org.knime.bigdata.spark2_4.converter.type.SerializableTypeProxies.FloatTypeProxy;
import org.knime.bigdata.spark2_4.converter.type.SerializableTypeProxies.IntegerTypeProxy;
import org.knime.bigdata.spark2_4.converter.type.SerializableTypeProxies.LongTypeProxy;
import org.knime.bigdata.spark2_4.converter.type.SerializableTypeProxies.NullTypeProxy;
import org.knime.bigdata.spark2_4.converter.type.SerializableTypeProxies.ShortTypeProxy;
import org.knime.bigdata.spark2_4.converter.type.SerializableTypeProxies.StringTypeProxy;
import org.knime.bigdata.spark2_4.converter.type.SerializableTypeProxies.TimestampTypeProxy;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class DefaultSpark_2_4_IntermediateToSparkConverterProvider extends DefaultIntermediateToSparkConverterProvider {


    /**
     * Constructor.
     */
    public DefaultSpark_2_4_IntermediateToSparkConverterProvider() {
        super(Spark_2_4_CompatibilityChecker.INSTANCE,
                new DefaultIntermediateToSparkConverter<>(IntermediateDataTypes.BINARY, new BinaryTypeProxy()),
                new DefaultIntermediateToSparkConverter<>(IntermediateDataTypes.BOOLEAN, new BooleanTypeProxy()),
                new DefaultIntermediateToSparkConverter<>(IntermediateDataTypes.BYTE, new ByteTypeProxy()),
                new DefaultIntermediateToSparkConverter<>(IntermediateDataTypes.CALENDAR_INTERVAL, new StringTypeProxy()),
                new DefaultIntermediateToSparkConverter<>(IntermediateDataTypes.DATE, new DateTypeProxy()),
                new DefaultIntermediateToSparkConverter<>(IntermediateDataTypes.DOUBLE, new DoubleTypeProxy()),
                new DefaultIntermediateToSparkConverter<>(IntermediateDataTypes.FLOAT, new FloatTypeProxy()),
                new DefaultIntermediateToSparkConverter<>(IntermediateDataTypes.INTEGER, new IntegerTypeProxy()),
                new DefaultIntermediateToSparkConverter<>(IntermediateDataTypes.LONG, new LongTypeProxy()),
                new DefaultIntermediateToSparkConverter<>(IntermediateDataTypes.NULL, new NullTypeProxy()),
                new DefaultIntermediateToSparkConverter<>(IntermediateDataTypes.SHORT, new ShortTypeProxy()),
                new DefaultIntermediateToSparkConverter<>(IntermediateDataTypes.STRING, new StringTypeProxy()),
                new DefaultIntermediateToSparkConverter<>(IntermediateDataTypes.TIMESTAMP, new TimestampTypeProxy()),
                new ToStringConverter());
    }
}
