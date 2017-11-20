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
package org.knime.bigdata.spark2_2.converter.type;

import java.io.Serializable;

import org.apache.spark.sql.types.StringType;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.converter.spark.AnyIntermediateToSparkConverter;
import org.knime.bigdata.spark.core.types.converter.spark.DefaultIntermediateToSparkConverter;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import org.knime.bigdata.spark2_2.converter.type.SerializableTypeProxies.StringTypeProxy;

/**
 * Converter that converts any intermediate value to a string and vice versa.
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class ToStringConverter extends DefaultIntermediateToSparkConverter<StringType>
    implements AnyIntermediateToSparkConverter<StringType> {

    private static final long serialVersionUID = 1L;

    /**
     * Default constructor necessary for extension point registration.
     */
    public ToStringConverter() {
        super(IntermediateDataTypes.ANY, new StringTypeProxy());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object convert(final Serializable intermediateTypeValue) {
        return intermediateTypeValue.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Serializable convert(final Object sparkObject) {
        return sparkObject.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntermediateDataType getActualIntermediateType() {
        return IntermediateDataTypes.STRING;
    }
}
