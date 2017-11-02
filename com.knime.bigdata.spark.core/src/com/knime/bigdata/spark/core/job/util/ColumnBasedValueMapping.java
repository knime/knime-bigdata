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
 *   Created on 21.08.2015 by koetter
 */
package com.knime.bigdata.spark.core.job.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class ColumnBasedValueMapping implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<Integer, Map<Serializable, Serializable>> m_map = new HashMap<>();

    private final List<Integer> m_colIdxs = new LinkedList<>();

    /**
     * @param colIdx the index of the column this mapping belongs to
     * @param input the input value
     * @param output the value to map to
     */
    public void add(final int colIdx, final Serializable input, final Serializable output) {
        final Integer idx = Integer.valueOf(colIdx);
        Map<Serializable, Serializable> map = m_map.get(idx);
        if (map == null) {
            map = new HashMap<>();
            m_colIdxs.add(idx);
            m_map.put(idx, map);
        }
        map.put(input, output);
    }

    /**
     * @return the indices of the columns a mapping exists for
     */
    public List<Integer> getColumnIndices() {
        return m_colIdxs;
    }

    /**
     * @param idx the column index the value belongs to
     * @param value the value to map
     * @return the mapped value or <code>null</code> if no mapping exists
     */
    public Object map(final Integer idx, final Object value) {
        final Map<Serializable, Serializable> map = m_map.get(idx);
        return map != null ? map.get(value) : null;
    }

    /**
     *
     * @return all value mappings for all columns
     */
    public Map<Integer, Map<Serializable, Serializable>> getAllMappings() {
        return m_map;
    }
}
