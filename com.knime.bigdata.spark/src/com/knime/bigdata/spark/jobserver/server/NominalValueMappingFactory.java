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
 *   Created on 20.07.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.server;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 * @author dwk
 */
public class NominalValueMappingFactory {

    /**
     *
     * @param aMappings
     * @return new global mapping of string to nominal values
     */
    public static NominalValueMapping createGlobalMapping(final Map<String, Integer> aMappings) {
        return new GlobalMapping(aMappings);
    }

    /**
     *
     * @param aMappings
     * @return new column local mapping of string to nominal values
     */
    public static NominalValueMapping createColumnMapping(final Map<Integer, Map<String, Integer>> aMappings) {
        return new ColumnMapping(aMappings);
    }

    private static class GlobalMapping implements NominalValueMapping {
        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((m_globalMappings == null) ? 0 : m_globalMappings.hashCode());
            return result;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            GlobalMapping other = (GlobalMapping)obj;
            if (m_globalMappings == null) {
                if (other.m_globalMappings != null) {
                    return false;
                }
            } else if (!m_globalMappings.equals(other.m_globalMappings)) {
                return false;
            }
            return true;
        }

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

        private List<MyRecord> toList() {
            final List<MyRecord> values = new ArrayList<>(size());
            for (Entry<String, Integer> entry : m_globalMappings.entrySet()) {
                values.add(new MyRecord(-1, entry.getKey(), entry.getValue()));
            }
            return values;
        }

        @Override
        public Iterator<MyRecord> iterator() {
            return new Iterator<MyRecord>() {
                final List<MyRecord> values = toList();

                @Override
                public boolean hasNext() {
                    return values.size() > 0;
                }

                @Override
                public MyRecord next() {
                    if (values.size() > 0) {
                        return values.remove(0);
                    }
                    throw new IllegalStateException("Iterator has no more elements");
                }

                @Override
                public void remove() {
                    next();
                }
            };
        }

    }

    private static class ColumnMapping implements NominalValueMapping {
        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((m_colMappings == null) ? 0 : m_colMappings.hashCode());
            return result;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ColumnMapping other = (ColumnMapping)obj;
            if (m_colMappings == null) {
                if (other.m_colMappings != null) {
                    return false;
                }
            } else if (!m_colMappings.equals(other.m_colMappings)) {
                return false;
            }
            return true;
        }

        private static final long serialVersionUID = 1L;

        final Map<Integer, Map<String, Integer>> m_colMappings;

        ColumnMapping(final Map<Integer, Map<String, Integer>> aMappings) {
            m_colMappings = aMappings;
        }

        @Override
        public Integer getNumberForValue(final int aColumnIx, final String aValue) {
            Map<String, Integer> m = m_colMappings.get(aColumnIx);
            if (m != null && m.containsKey(aValue)) {
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

        private List<MyRecord> toList() {
            final List<MyRecord> values = new ArrayList<>(size());
            for (Entry<Integer, Map<String, Integer>> colEntries : m_colMappings.entrySet()) {
                for (Entry<String, Integer> entry : colEntries.getValue().entrySet()) {
                    values.add(new MyRecord(colEntries.getKey(), entry.getKey(), entry.getValue()));
                }
            }
            return values;
        }

        @Override
        public Iterator<MyRecord> iterator() {
            return new Iterator<MyRecord>() {
                final List<MyRecord> values = toList();

                @Override
                public boolean hasNext() {
                    return values.size() > 0;
                }

                @Override
                public MyRecord next() {
                    if (values.size() > 0) {
                        return values.remove(0);
                    }
                    throw new IllegalStateException("Iterator has no more elements");
                }

                @Override
                public void remove() {
                    next();
                }
            };
        }

    }
}
