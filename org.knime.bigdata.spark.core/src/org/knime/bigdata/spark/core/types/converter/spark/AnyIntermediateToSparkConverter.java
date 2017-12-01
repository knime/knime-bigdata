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
 *   Created on May 5, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.types.converter.spark;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;

/**
 * Interface for {@link IntermediateToSparkConverter}s that convert {@link IntermediateDataTypes#ANY} to a Spark data
 * type. For these it is not clear which actual {@link IntermediateDataType} they convert to. For each Spark version
 * there must only be a single converter that implements this interface.
 *
 * @author Bjoern Lohrmann
 * @param <T> the actual Spark data type which might be Spark version dependent
 */
@SparkClass
public interface AnyIntermediateToSparkConverter<T> extends IntermediateToSparkConverter<T> {

    /**
     * @return the actual {@link IntermediateDataType} this converter converts Spark values to, which is otherwise
     *         unclear for {@link IntermediateToSparkConverter}s that with intermediate type
     *         {@link IntermediateDataTypes#ANY}.
     */
    public IntermediateDataType getActualIntermediateType();
}
