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
package org.knime.bigdata.spark.core.types.converter.spark;

import java.io.Serializable;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.TypeConverter;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 * @param <T> the actual Spark data type which might be Spark version dependent
 */
@SparkClass
public interface IntermediateToSparkConverter<T> extends TypeConverter, Serializable{

    /**
     * @return the {@link IntermediateDataType}
     */
    public IntermediateDataType getIntermediateDataType();

    /**
     * @return the actual Spark data type
     */
    public T getSparkDataType();

    /**
     * @return the intermediate type
     */
    public Serializable getSerializableDataType();

    /**
     * @param intermediateTypeValue the intermediate value to convert
     * @return the converted intermediate value
     */
    public Object convert(Serializable intermediateTypeValue);

    /**
     * @param sparkObject the spark value to convert into the intermediate value
     * @return the intermediate value
     */
    public Serializable convert(Object sparkObject);

}
