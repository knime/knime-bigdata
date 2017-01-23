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
 *   Created on May 9, 2016 by oole
 */
package com.knime.bigdata.spark.node.preproc.convert.category2number;

import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.preproc.convert.NominalValueMapping;

/**
 *
 * @author oole
 */
@SparkClass
public class Category2NumberJobOutput extends JobOutput {

    private static final String KEY_APPENDED_COL_NAMES = "appendedColNames";
    private static final String KEY_MAPPINGS = "mappings";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public Category2NumberJobOutput(){}

    /**
     * @param appendedColNames the appended column names
     * @param mappings the {@link NominalValueMapping}
     */
    public Category2NumberJobOutput(final String appendedColNames[], final NominalValueMapping mappings) {
        set(KEY_APPENDED_COL_NAMES, appendedColNames);
        set(KEY_MAPPINGS, mappings);
    }

    /**
     * @return appended column names (mapped columns)
     */
    public String[] getAppendedColumnsNames(){
        return get(KEY_APPENDED_COL_NAMES);
    }

    /**
     * @return the {@link NominalValueMapping}
     */
    public NominalValueMapping getMappings() {
        return get(KEY_MAPPINGS);
    }

}
