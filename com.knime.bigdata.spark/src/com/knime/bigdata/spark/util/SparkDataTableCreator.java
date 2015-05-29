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
 *   Created on 28.05.2015 by koetter
 */
package com.knime.bigdata.spark.util;

import java.util.Iterator;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowIterator;
import org.knime.core.data.RowKey;
import org.knime.core.node.NodeLogger;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.FetchRowsJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;

/**
 *
 * @author koetter
 */
public final class SparkDataTableCreator {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkDataTableCreator.class);


    /**
     * Retrieves all rows of the given rdd and converts them into a KNIME data table
     * @param sparkTableName the unique name of the rdd
     * @param spec the {@link DataTableSpec} of the rdd
     * @return the named RDD as a DataTable
     */
    public static DataTable getDataTable(final String sparkTableName, final DataTableSpec spec) {
        return getDataTable(sparkTableName, spec, -1);
    }

    /**
     * Retrieves the given number of rows from the given rdd and converts them into a KNIME data table
     * @param sparkTableName the unique name of the rdd
     * @param spec the {@link DataTableSpec} of the rdd
     * @param cacheNoRows the number of rows to retrieve
     * @return the named RDD as a DataTable
     */
    public static DataTable getDataTable(final String sparkTableName, final DataTableSpec spec, final int cacheNoRows) {
        try {
            String contextName = KnimeContext.getSparkContext();

            final String fetchParams = rowFetcherDef(cacheNoRows, sparkTableName);

            String jobId = JobControler.startJob(contextName, FetchRowsJob.class.getCanonicalName(), fetchParams);

            JobControler.waitForJob(jobId, null);

            assert (JobStatus.OK != JobControler.getJobStatus(jobId));

            return convertResultToDataTable(jobId, spec);
        } catch (Throwable t) {
            LOGGER.error("Could not fetch data from Spark RDD, reason: " + t.getMessage(), t);
            return null;
        }
    }

    private static String rowFetcherDef(final int aNumRows, final String aTableName) {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_NUMBER_ROWS, "" + aNumRows, ParameterConstants.PARAM_DATA_PATH,
                aTableName}});
    }

    private static DataTable convertResultToDataTable(final String aJobId, final DataTableSpec spec)
            throws GenericKnimeSparkException {
        // now check result:
        JobResult statusWithResult = JobControler.fetchJobResult(aJobId);
         if (!"OK".equals(statusWithResult.getMessage())) {
             //fetcher should return OK as result status
             throw new GenericKnimeSparkException(statusWithResult.getMessage());
         }
        final Object[][] arrayRes = (Object[][])statusWithResult.getObjectResult();
        assert (arrayRes != null) : "Row fetcher failed to return a result";

        return new DataTable() {

            @Override
            public RowIterator iterator() {
                return new RowIterator() {

                    private int currentRow = 0;

                    @Override
                    public DataRow next() {
                        final Object[] o = arrayRes[currentRow];
                        currentRow++;
                        return new DataRow() {
                            private final RowKey m_rowKey = RowKey.createRowKey(currentRow - 1);
                            @Override
                            public Iterator<DataCell> iterator() {
                                return new Iterator<DataCell>() {
                                    private int current = 0;

                                    @Override
                                    public boolean hasNext() {
                                        return current < o.length;
                                    }

                                    @Override
                                    public DataCell next() {
                                        DataCell cell = getCell(current);
                                        current++;
                                        return cell;
                                    }

                                    @Override
                                    public void remove() {
                                        throw new UnsupportedOperationException();
                                    }
                                };
                            }

                            @Override
                            public int getNumCells() {
                                return o.length;
                            }

                            @Override
                            public RowKey getKey() {
                                return m_rowKey;
                            }

                            @Override
                            public DataCell getCell(final int index) {
                                //TK_TODO: Generate the right DataCells e.g. DoubleCell, etc. based on the TableSpec
                                return new MyRDDDataCell(o, index);
                            }
                        };
                    }

                    @Override
                    public boolean hasNext() {
                        return currentRow < arrayRes.length;
                    }
                };
            }

            @Override
            public DataTableSpec getDataTableSpec() {
                return spec;
            }
        };
    }

    private static class MyRDDDataCell extends DataCell {
        private final int m_index;
        private final Object[] m_row;

        MyRDDDataCell(final Object[] aRow, final int aIndex) {
            m_index = aIndex;
            m_row = aRow;
        }
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        @Override
        public String toString() {
            return m_row[m_index].toString();
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }

        @Override
        protected boolean equalsDataCell(final DataCell dc) {
            return (dc != null && dc.toString().equals(toString()));
        }
    }
}
