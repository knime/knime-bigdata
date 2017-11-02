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
 *   Created on 20.08.2015 by koetter
 */
package com.knime.bigdata.spark.core.job.util;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class NominalFeatureInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**Empty {@link NominalFeatureInfo} class.*/
    public static final NominalFeatureInfo EMPTY = new NominalFeatureInfo();

    private final Map<Integer, Integer> m_info = new LinkedHashMap<>();

    /**
     * @param colIdx the index of the nominal column in the input feature vector
     * @param numberOfVals the number of unique values within the nominal column
     */
    public void add(final Integer colIdx, final Integer numberOfVals) {
        m_info.put(colIdx, numberOfVals);
    }

    /**
     * @return an unmodifiable map that contains the nominal column index as key and the number of unique values
     * within this column as value
     */
    public Map<Integer, Integer> getMap() {
        return Collections.unmodifiableMap(m_info);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "NominalFeatureInfo [m_info=" + m_info + "]";
    }
}
