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
 *   Created on 16.07.2015 by Dietrich
 */
package com.knime.bigdata.spark.jobserver.server;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 * @author dwk
 */
public interface NominalValueMapping extends Serializable {
    /**
     *
     * @param aColumnIx optional column index, ignored for global mappings, required
     * for column mapping
     * @param aValue the value to be mapped
     * @return the number for the given value
     */
    Integer getNumberForValue(final int aColumnIx, final String aValue);

    /**
     * for global mappings this is the size of the of the global map, for column mapping, it is
     * the sum of the sizes of the maps for the separate columns
     * @return number of distinct values
     */
    int size();

    /**
     * number of values for this column (not supported for global mapping)
     * @param aNominalColumnIx
     * @return number of distinct values at this column
     */
    int getNumberOfValues(int aNominalColumnIx);

}

class GlobalMapping implements NominalValueMapping {
    private static final long serialVersionUID = 1L;
    final Map<String, Integer> m_globalMappings;

    GlobalMapping(final Map<String, Integer> aMappings) {
        m_globalMappings = aMappings;
    }

    @Override
    public Integer getNumberForValue(final int aColumnIx, final String aValue) {
        return m_globalMappings.get(aValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return m_globalMappings.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfValues(final int aNominalColumnIx) {
        return 0;
    }
}

class ColumnMapping implements NominalValueMapping {
    private static final long serialVersionUID = 1L;
    final Map<Integer, Map<String, Integer>> m_colMappings;

    ColumnMapping(final Map<Integer, Map<String, Integer>> aMappings) {
        m_colMappings = aMappings;
    }

    @Override
    public Integer getNumberForValue(final int aColumnIx, final String aValue) {
        Map<String, Integer> m = m_colMappings.get(aColumnIx);
        if (m != null && m.containsKey(aValue))
        {
            return m.get(aValue);
        }
        return -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        int s = 0;
        for (Entry<Integer, Map<String, Integer>> entry : m_colMappings.entrySet()) {
            s += entry.getValue().size();
        }
        return s;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfValues(final int aNominalColumnIx) {
        return m_colMappings.get(aNominalColumnIx).size();
    }

}
