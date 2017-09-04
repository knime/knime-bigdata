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
 *   Created on 29.05.2015 by koetter
 */
package com.knime.bigdata.spark.core.port.data;

import org.osgi.framework.Version;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;

/**
 * This class represents a data object within Spark. The object is identified by the Spark context
 * ({@link #getContextID()}) it lives in and its unique id ({@link #getID()}).
 *
 * @author Tobias Koetter, KNIME.com
 */
public interface SparkData {

    /**
     * @return the unique id of the Spark object
     */
    public abstract String getID();

    /**
     * @return the Spark context the object lives in
     */
    public abstract SparkContextID getContextID();

    /**
     * @param other the other {@link SparkData} to check for compatibility
     * @return <code>true</code> if both are compatible e.g. live in the same context
     */
    public boolean compatible(final SparkData other);


    /**
     * Each {@link SparkData} object is created by a KNIME node. This method returns the version of KNIME Spark Executor
     * that the respective node model was first instantiated with. This is relevant for example when retrieving type
     * converters with {@link KNIMEToIntermediateConverterRegistry#get(org.knime.core.data.DataType, Version)}.
     *
     * @return The OSGI {@link Version} of KNIME Spark Executor with which this data object was created.
     * @since 2.1.0
     * @see KNIMEToIntermediateConverterRegistry#get(org.knime.core.data.DataType, Version)
     */
    public Version getKNIMESparkExecutorVersion();

}