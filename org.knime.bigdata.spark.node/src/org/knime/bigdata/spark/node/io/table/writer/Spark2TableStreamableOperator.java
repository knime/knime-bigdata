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
 *   Created on Aug 26, 2016 by bjoern
 */
package org.knime.bigdata.spark.node.io.table.writer;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.RowIterator;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.streamable.StreamableOperator;

/**
 * A {@link StreamableOperator} implementation for the {@link Spark2TableNodeModel}. This class supports KNIME-on-Spark
 * mode.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class Spark2TableStreamableOperator extends AbstractSpark2TableStreamableOperator {

    private final int m_fetchSize;

    /**
     * Creates a streamable operator where rows will be fetched from an RDD/Dataset in a remote Spark context into the
     * local main memory and then converted into {@link DataRow}s. The streamable operator returned by this constructor
     * will fetch the whole RDD/Dataset.
     */
    public Spark2TableStreamableOperator() {
        this(-1);
    }

    /**
     * Creates a streamable operator where rows will be fetched from an RDD/Dataset in a remote Spark context into the
     * local main memory and then converted into {@link DataRow}s. The streamable operator returned by this constructor
     * will at maximum fetch the specified number of rows.
     *
     * @param fetchSize The maximum number of rows to fetch.
     */
    public Spark2TableStreamableOperator(final int fetchSize) {
        m_fetchSize = fetchSize;
    }

    private boolean shouldFetchAllRows() {
        return m_fetchSize == -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RowIterator getRowIterator(final SparkDataTable sparkDataTable, final ExecutionContext exec)
        throws InterruptedException, CanceledExecutionException, KNIMESparkException {

        exec.setMessage("Retrieving data from Spark...");
        final DataTable dataTable;
        if (shouldFetchAllRows()) {
            dataTable = SparkDataTableUtil.getDataTable(exec, sparkDataTable);
        } else {
            dataTable = SparkDataTableUtil.getDataTable(exec, sparkDataTable, m_fetchSize);
        }
        return dataTable.iterator();
    }
}
