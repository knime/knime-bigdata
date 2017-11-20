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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark.node.preproc.convert.number2category;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class Number2CategoryJobInput extends JobInput {

    private static final String MAPPING = "mapping";
    private static final String KEEP_ORIGINAL = "keepOriginal";
    private static final String COL_SUFFIX = "colSuffix";

    /**
     * Paramless constructor for automatic deserialization
     */
    public Number2CategoryJobInput() {}


    /**
     * @param namedInputObject
     * @param map
     * @param keepOriginalColumns
     * @param colSuffix
     * @param namedOutputObject
     */
    public Number2CategoryJobInput(final String namedInputObject, final ColumnBasedValueMapping map,
            final boolean keepOriginalColumns, final String colSuffix, final String namedOutputObject) {
        addNamedInputObject(namedInputObject);
        addNamedOutputObject(namedOutputObject);
        set(MAPPING, map);
        set(KEEP_ORIGINAL,keepOriginalColumns);
        set(COL_SUFFIX, colSuffix);
    }



    /**
     * @return the ColumnbasedValueMapping
     */
    public ColumnBasedValueMapping getMapping() {
        return get(MAPPING);
    }

    /**
     * @return whether the original columns should be kept
     */
    public boolean keepOriginalColumns() {
        return get(KEEP_ORIGINAL);
    }

    /**
     * @return column suffix
     */
    public String getColSuffix() {
        return get(COL_SUFFIX);
    }
}
