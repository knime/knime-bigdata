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
 *   Created on 16.07.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.server;

import java.io.Serializable;
import java.util.Iterator;

/**
 *
 * @author dwk
 */
public interface NominalValueMapping extends Serializable {
    /**
     *
     * @param aColumnIx optional column index, ignored for global mappings, required for column mapping
     * @param aValue the value to be mapped
     * @return the number for the given value
     */
    Integer getNumberForValue(final int aColumnIx, final String aValue);

    /**
     * for global mappings this is the size of the of the global map, for column mapping, it is the sum of the sizes of
     * the maps for the separate columns
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
     * @return iterator over all columns and value of this record, columns are -1 for global mapping
     */
    Iterator<MyRecord> iterator();

    /**
     * post fix for numeric column names (binary columns have a '_' plus the nominal value as a post-fix)
     */
    final static String NUMERIC_COLUMN_NAME_POSTFIX = "_num";


}
