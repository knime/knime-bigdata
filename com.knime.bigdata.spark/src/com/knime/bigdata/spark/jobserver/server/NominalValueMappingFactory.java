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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.spark.sql.api.java.Row;

/**
 *
 * @author dwk
 */
public class NominalValueMappingFactory {

    /**
     *
     * @param aMappings
     * @param aMappingType
     * @return new column local mapping of string to nominal values
     */
    public static NominalValueMapping createColumnMapping(final Map<Integer, Map<String, Integer>> aMappings,
        final MappingType aMappingType) {
        return new ColumnMapping(aMappings, aMappingType);
    }

    private static abstract class Mapping implements NominalValueMapping {
        private static final long serialVersionUID = 1L;

        private final MappingType m_mappingType;

        /**
         * @param aMappingType
         */
        Mapping(final MappingType aMappingType) {
            m_mappingType = aMappingType;
        }

        @Override
        public MappingType getType() {
            return m_mappingType;
        }

        protected abstract List<MyRecord> toList();

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

    private static class ColumnMapping extends Mapping {
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

        private final Map<Integer, Map<String, Integer>> m_colMappings;

        ColumnMapping(final Map<Integer, Map<String, Integer>> aMappings, final MappingType aMappingType) {
            super(aMappingType);
            m_colMappings = Collections.unmodifiableMap(aMappings);
        }

        @Override
        public Integer getNumberForValue(final int aColumnIx, final String aValue) throws NoSuchElementException {
            Map<String, Integer> m = m_colMappings.get(aColumnIx);
            if (m != null && m.containsKey(aValue)) {
                return m.get(aValue);
            }
            throw new NoSuchElementException("ERROR: no mapping available for nominal column "+aColumnIx+" and value '"+aValue+"'.");
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

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasMappingForColumn(final int aNominalColumnIx) {
            return m_colMappings.containsKey(aNominalColumnIx);
        }

        @Override
        protected List<MyRecord> toList() {
            final List<MyRecord> values = new ArrayList<>(size());
            for (Entry<Integer, Map<String, Integer>> colEntries : m_colMappings.entrySet()) {
                for (Entry<String, Integer> entry : colEntries.getValue().entrySet()) {
                    values.add(new MyRecord(colEntries.getKey(), entry.getKey(), entry.getValue()));
                }
            }
            Collections.sort(values, new MyComparator());
            return values;
        }

        private static class MyComparator implements Comparator<MyRecord>, Serializable {
            private static final long serialVersionUID = 1L;

            /**
             * sort first by initial column index, then by mapped number value {@inheritDoc}
             */
            @Override
            public int compare(final MyRecord aRecord0, final MyRecord aRecord1) {
                if (aRecord0.m_nominalColumnIndex < aRecord1.m_nominalColumnIndex) {
                    return -1;
                }
                if (aRecord0.m_nominalColumnIndex > aRecord1.m_nominalColumnIndex) {
                    return 1;
                }
                if (aRecord0.m_numberValue < aRecord1.m_numberValue) {
                    return -1;
                }
                if (aRecord0.m_numberValue > aRecord1.m_numberValue) {
                    return 1;
                }
                return 0;
            }
        }
    }

    /**
     * @param aMappingsTable table with mapping information (as created by
     *            com.knime.bigdata.spark.jobserver.server.MappedRDDContainer.createMappingTable(Map<Integer, String>,
     *            int))
     * @return NominalValueMapping of correct mapping type
     */
    public static NominalValueMapping fromTable(final List<Row> aMappingsTable) {
        Map<Integer, Map<String, Integer>> mappings = new HashMap<>();
        int numCol = 0;
        for (Row r : aMappingsTable) {
            // see com.knime.bigdata.spark.jobserver.server.MappedRDDContainer.createMappingTable(Map<Integer, String>, int)
            // builder.add(name).add(record.m_nominalColumnIndex).add(record.m_nominalValue).add(record.m_numberValue);
            final String name = r.getString(0);
            if (name.endsWith(NominalValueMapping.NUMERIC_COLUMN_NAME_POSTFIX)) {
                numCol++;
            }
            int colIx = r.getInt(1);
            Map<String, Integer> colMappings = mappings.get(colIx);
            if (colMappings == null) {
                colMappings = new HashMap<String, Integer>();
            }
            colMappings.put(r.getString(2), r.getInt(3));
            mappings.put(colIx, colMappings);
        }
        //we don't care whether it is global or column - makes no difference
        // only binary is different
        final MappingType mappingType;
        if (numCol < aMappingsTable.size() / 2) {
            mappingType = MappingType.BINARY;
        } else {
            mappingType = MappingType.COLUMN;
        }
        return createColumnMapping(mappings, mappingType);
    }
}
