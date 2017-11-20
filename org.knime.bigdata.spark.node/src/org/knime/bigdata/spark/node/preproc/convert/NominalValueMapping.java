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
 *   Created on 16.07.2015 by dwk
 */
package com.knime.bigdata.spark.node.preproc.convert;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;

/**
 *
 * @author dwk
 */
@SparkClass
public interface NominalValueMapping extends Serializable {
    /**
     *
     * @param aColumnIx optional column index, ignored for global mappings, required for column mapping
     * @param aValue the value to be mapped
     * @return the number for the given value
     * @throws NoSuchElementException thrown if either no mapping exists for column index (use hasMappingForColumn to
     *             check before calling this method) or no mapping is known for value
     */
    Integer getNumberForValue(final int aColumnIx, final String aValue) throws NoSuchElementException;

    /**
     * the sum of the number of distinct values in each mapped column, regardless of mapping type
     *
     * @return number of distinct values
     */
    int size();

    /**
     * number of values for this column (not supported for global mapping)
     *
     * @param aNominalColumnIx
     * @return number of distinct values at this column
     */
    int getNumberOfValues(int aNominalColumnIx);

    /**
     *
     * @param aNominalColumnIx
     * @return true if there is a mapping for the given column index
     */
    boolean hasMappingForColumn(int aNominalColumnIx);

    /**
     * @return iterator over all columns and value of this record, columns are -1 for global mapping, records are sorted
     *         by nominal column and mapped value
     */
    Iterator<MyRecord> iterator();

    /**
     * @return the type of mapping
     */
    MappingType getType();

    /**
     * post fix for numeric column names (binary columns have a '_' plus the nominal value as a post-fix)
     */
    final static String NUMERIC_COLUMN_NAME_POSTFIX = "_num";

}
