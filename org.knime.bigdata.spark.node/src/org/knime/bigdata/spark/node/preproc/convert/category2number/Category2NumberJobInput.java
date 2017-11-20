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
package org.knime.bigdata.spark.node.preproc.convert.category2number;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.EnumContainer.MappingType;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class Category2NumberJobInput extends JobInput {

    /**
     * type of mapping
     */
    private static final String MAPPING_TYPE = "MappingType";

    private static final String COL_SUFFIX = "colSuffix";

    private static final String INCLUDE_COL_IDXS = "includeColIdxs";

    private static final String INCLUDE_COL_NAMES = "includeColNames";

    private static final String KEEP_ORIGINAL = "keepOriginal";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public Category2NumberJobInput(){}

    /**
     * constructor - simply stores parameters
     *
     * @param namedInputObject input RDD
     * @param includeColIdxs - indices of the columns to include starting with 0
     * @param includedColsNames
     * @param mappingType - type of value mapping (global, per column or binary)
     * @param keepOriginalColumns  keep original columns or not, default is true
     * @param colSuffix the column name suffix to use for none binary mappings
     * @param namedOutputObject - table identifier (output data)
     */
    public Category2NumberJobInput(final String namedInputObject, final Integer[] includeColIdxs,
            final String[] includedColsNames, final MappingType mappingType, final boolean keepOriginalColumns,
            final String colSuffix, final String namedOutputObject) {

        addNamedInputObject(namedInputObject);
        addNamedOutputObject(namedOutputObject);
        set(INCLUDE_COL_IDXS, ArrayUtils.toPrimitive(includeColIdxs));
        set(INCLUDE_COL_NAMES, includedColsNames);
        set(MAPPING_TYPE, mappingType.name());
        set(KEEP_ORIGINAL, keepOriginalColumns);
        set(COL_SUFFIX, colSuffix);
    }

    /**
     * @return the indices of the columns to include
     */
    public int[] getIncludeColIdxs() {
        return get(INCLUDE_COL_IDXS);
    }

    /**
     * @return the names of the columns to include
     */
    public String[] getIncludeColNames() {
        return get(INCLUDE_COL_NAMES);
    }

    /**
     * @return the value mapping type
     */
    public MappingType getMappingType() {
        final String typeName = get(MAPPING_TYPE);
        return MappingType.valueOf(typeName);
    }

    /**
     * @return <code>true</code> if the original columns should be retained
     */
    public boolean keepOriginalCols() {
        return get(KEEP_ORIGINAL);
    }

    /**
     * @return the column suffix
     */
    public String getColSuffix() {
        return get(COL_SUFFIX);
    }
}
