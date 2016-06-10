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
 *   Created on 26.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.statistics.compute;

import java.util.Iterator;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;

import com.knime.bigdata.spark.core.job.util.MLlibSettings;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class StatsRowCreator implements Iterable<DataRow>{

    private static final int MIN = 0;
    private static final int MAX = 1;
    private static final int MEAN = 2;
    private static final int VARIANCE = 3;
    private static final int L1 = 4;
    private static final int L2 = 5;
    private static final int NONZEROS = 6;

    private static final String[] COL_NAMES = new String[] {"Min", "Max", "Mean", "Variance", "L1", "L2", "Nonzeros"};

    private double[][] m_vals = new double[7][];
    private long m_rowCount;
    private MLlibSettings m_settings;

    /**
     * @param stats
     * @param settings
     */
    public StatsRowCreator(final StatisticsJobOutput stats, final MLlibSettings settings) {
        m_settings = settings;
        m_rowCount = stats.count();
        m_vals[MAX] = stats.max();
        m_vals[MEAN] = stats.mean();
        m_vals[MIN] = stats.min();
        m_vals[L1] = stats.normL1();
        m_vals[L2] = stats.normL2();
        m_vals[NONZEROS] = stats.numNonzeros();
        m_vals[VARIANCE] = stats.variance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<DataRow> iterator() {
        return new Iterator<DataRow>() {
            private int m_idx = 0;
            /**
             * {@inheritDoc}
             */
            @Override
            public boolean hasNext() {
                return m_idx < m_settings.getFatureColNames().size();
            }
            /**
             * {@inheritDoc}
             */
            @Override
            public DataRow next() {
                final String colName = m_settings.getFatureColNames().get(m_idx);
                final DataCell[] cells = new DataCell[m_vals.length + 3];
                cells[0] = new StringCell(colName);
                for (int i = 1; i <= m_vals.length; i++) {
                    final DataCell cell;
                    double val = m_vals[i - 1][m_idx];
                    if (i - 1 == NONZEROS) {
                        cell = new LongCell((long)val);
                    } else {
                        cell = new DoubleCell(val);
                    }
                    cells[i] = cell;
                }
                cells[cells.length - 2] = new LongCell(m_rowCount - (long)m_vals[NONZEROS][m_idx]);
                cells[cells.length - 1] = new LongCell(m_rowCount);
                m_idx++;
                return new DefaultRow(new RowKey(colName), cells);
            }
            /**
             * {@inheritDoc}
             */
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * @return the {@link DataTableSpec}
     */
    public static DataTableSpec createSpec() {
        DataColumnSpec[] specs = new DataColumnSpec[COL_NAMES.length + 3];
        final DataColumnSpecCreator creator = new DataColumnSpecCreator("Column", StringCell.TYPE);
        specs[0] = creator.createSpec();
        for (int i = 1; i <= COL_NAMES.length; i++) {
            creator.setName(COL_NAMES[i - 1]);
            if (i - 1 == NONZEROS) {
                creator.setType(LongCell.TYPE);
            } else {
                creator.setType(DoubleCell.TYPE);
            }
            specs[i] = creator.createSpec();
        }
        creator.setName("Zeros");
        creator.setType(LongCell.TYPE);
        specs[specs.length - 2] = creator.createSpec();
        creator.setName("Row count");
        creator.setType(LongCell.TYPE);
        specs[specs.length - 1] = creator.createSpec();
        return new DataTableSpec(specs);
    }

    /**
     * @param exec {@link ExecutionContext} to provide progress and to create table
     * @return {@link BufferedDataTable} with statistics
     * @throws CanceledExecutionException if operation has been canceled
     */
    public BufferedDataTable createTable(final ExecutionContext exec) throws CanceledExecutionException {
        final BufferedDataContainer dc = exec.createDataContainer(createSpec());
        int counter = 0;
        int size = m_settings.getFatureColNames().size();
        for (final DataRow row : this) {
            exec.setProgress(counter / (double) size, "Writing row " + counter + " of " + size);
            exec.checkCanceled();
            dc.addRowToTable(row);
        }
        dc.close();
        return dc.getTable();
    }

}