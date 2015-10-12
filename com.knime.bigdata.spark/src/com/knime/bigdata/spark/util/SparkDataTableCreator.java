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
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.FetchRowsJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.converter.SparkTypeConverter;
import com.knime.bigdata.spark.util.converter.SparkTypeRegistry;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public final class SparkDataTableCreator {

    /**
     * Retrieves all rows of the given rdd and converts them into a KNIME data table
     *
     * @param exec optional ExecutionMonitor to check for cancel. Can be <code>null</code>.
     * @param data the Spark data object
     * @return the named RDD as a DataTable
     * @throws GenericKnimeSparkException
     * @throws CanceledExecutionException
     */
    public static DataTable getDataTable(final ExecutionMonitor exec, final SparkDataTable data)
        throws CanceledExecutionException, GenericKnimeSparkException {
        return getDataTable(exec, data, -1);
    }

    /**
     * Retrieves the given number of rows from the given rdd and converts them into a KNIME data table
     *
     * @param exec optional ExecutionMonitor to check for cancel. Can be <code>null</code>.
     * @param data the Spark data object
     * @param cacheNoRows the number of rows to retrieve. Returns empty table if 0.
     * @return the named RDD as a DataTable
     * @throws CanceledExecutionException
     * @throws GenericKnimeSparkException
     */
    public static DataTable getDataTable(final ExecutionMonitor exec, final SparkDataTable data, final int cacheNoRows)
        throws CanceledExecutionException, GenericKnimeSparkException {
        if (cacheNoRows == 0) {
            //return an empty table
            return new DataTable() {
                @Override
                public RowIterator iterator() {
                    return new RowIterator() {

                        @Override
                        public DataRow next() {
                            return null;
                        }

                        @Override
                        public boolean hasNext() {
                            return false;
                        }
                    };
                }

                @Override
                public DataTableSpec getDataTableSpec() {
                    return data.getTableSpec();
                }
            };
        }
        final String fetchParams = rowFetcherDef(cacheNoRows, data.getID());
        final KNIMESparkContext context = data.getContext();
        if (exec != null) {
            exec.checkCanceled();
        }
        final JobResult result = JobControler.startJobAndWaitForResult(context, FetchRowsJob.class.getCanonicalName(),
            fetchParams, exec);
        return convertResultToDataTable(context, result, data.getTableSpec());
    }

    private static String rowFetcherDef(final int aNumRows, final String aTableName) {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_NUMBER_ROWS, "" + aNumRows, KnimeSparkJob.PARAM_INPUT_TABLE,
                aTableName}});
    }

    private static DataTable convertResultToDataTable(final KNIMESparkContext context, final JobResult statusWithResult,
        final DataTableSpec spec) throws GenericKnimeSparkException {
        // now check result:
        final String message = statusWithResult.getMessage();
        //TODO:  Returned message is "OK" and not OK
        if (!"\"OK\"".equals(message)) {
            //fetcher should return OK as result status
            throw new GenericKnimeSparkException(message);
        }
        final Object[][] arrayRes = (Object[][])statusWithResult.getObjectResult();
        assert (arrayRes != null) : "Row fetcher failed to return a result";

        final SparkTypeConverter<?, ?>[] converter = SparkTypeRegistry.getConverter(spec);

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
                                return converter[index].convert(o[index]);
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
}
