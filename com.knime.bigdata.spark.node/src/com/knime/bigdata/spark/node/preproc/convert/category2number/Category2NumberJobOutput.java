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

import java.util.Map;

import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.preproc.convert.NominalValueMapping;

/**
 *
 * @author oole
 */
@SparkClass
public class Category2NumberJobOutput extends JobOutput {

    private static final String COL_NAMES = "colNames";
    private static final String MAPPINGS = "mappings";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public Category2NumberJobOutput(){}
    
    /**
     * @param colNames the column names with mapping information
     * @param mappings the {@link NominalValueMapping}
     */
    public Category2NumberJobOutput( final Map<Integer, String> colNames, final NominalValueMapping mappings) {
        set(COL_NAMES, colNames);
        set(MAPPINGS, mappings);
    }

    /**
     * @return the column names map
     */
    public Map<Integer, String> getColumnsNames(){
        return get(COL_NAMES);
    }

    /**
     * @return the {@link NominalValueMapping}
     */
    public NominalValueMapping getMappings() {
        return get(MAPPINGS);
    }

}
