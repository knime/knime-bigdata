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
package com.knime.bigdata.spark.core.port.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowIterator;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.core.context.SparkContext.SparkContextStatus;
import com.knime.bigdata.spark.core.context.SparkContextConstants;
import com.knime.bigdata.spark.core.context.SparkContextManager;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.JobRunFactory;
import com.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverter;
import com.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public final class SparkDataTableUtil {

    /**
     * @param spec input {@link DataTableSpec}
     * @return the {@link DataTableSpec} based on the available Spark to KNIME converters
     */
    public static DataTableSpec getDataTableSpec(final DataTableSpec spec) {
        final List<DataColumnSpec> specs = new ArrayList<>(spec.getNumColumns());
        final DataColumnSpecCreator specCreator = new DataColumnSpecCreator("DUMMY", StringCell.TYPE);
        for (DataColumnSpec colSpec : spec) {
            final KNIMEToIntermediateConverter converter = KNIMEToIntermediateConverterRegistry.get(colSpec.getType());
            final DataType converterDataType = converter.getKNIMEDataType();
            if (converterDataType.equals(colSpec.getType())) {
                specs.add(colSpec);
            } else {
                specCreator.setName(colSpec.getName());
                specCreator.setType(converterDataType);
                specs.add(specCreator.createSpec());
            }
        }
        return new DataTableSpec(specs.toArray(new DataColumnSpec[0]));
    }

    /**
     * Retrieves all rows of the given rdd and converts them into a KNIME data table
     *
     * @param exec optional ExecutionMonitor to check for cancel. Can be <code>null</code>.
     * @param data the Spark data object
     * @return the named RDD as a DataTable
     * @throws KNIMESparkException
     * @throws CanceledExecutionException
     */
    public static DataTable getDataTable(final ExecutionMonitor exec, final SparkDataTable data)
        throws CanceledExecutionException, KNIMESparkException {
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
     * @throws KNIMESparkException
     */
    public static DataTable getDataTable(final ExecutionMonitor exec, final SparkDataTable data, final int cacheNoRows)
        throws CanceledExecutionException, KNIMESparkException {

        if (cacheNoRows == 0) {
            final DataTableSpec resultSpec = getDataTableSpec(data.getTableSpec());
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
                    return resultSpec;
                }
            };
        }
        if (exec != null) {
            exec.checkCanceled();
        }
        return fetchDataTable(data, cacheNoRows, exec);
    }

    /**
     * @param data
     * @param noOfRows
     * @param exec
     * @return
     * @throws CanceledExecutionException
     * @throws KNIMESparkException
     */
    private static DataTable fetchDataTable(final SparkDataTable data, final int noOfRows, final ExecutionMonitor exec)
            throws KNIMESparkException, CanceledExecutionException {
        final IntermediateSpec intermediateSpec = toIntermediateSpec(data.getTableSpec());
        final FetchRowsJobInput input = FetchRowsJobInput.create(noOfRows, data.getID(), intermediateSpec);
        if (SparkContextManager.getOrCreateSparkContext(data.getContextID()).getStatus() != SparkContextStatus.OPEN) {
            throw new KNIMESparkException("Spark context does not exist (anymore). Please reset all preceding nodes and rexecute them.");
        }
        final JobRunFactory<FetchRowsJobInput, FetchRowsJobOutput> execProvider =
            SparkContextUtil.getJobRunFactory(data.getContextID(), SparkContextConstants.FETCH_ROWS_JOB_ID);
        final FetchRowsJobOutput output = execProvider.createRun(input).run(data.getContextID(), exec);
        return convertResultToDataTable(output.getRows(), data.getTableSpec());
    }


    /**
     * @param intermediateTypeRows two dimensional array with the Java object representation of the SparkRDD rows
     * @param spec the {@link DataTableSpec} that contains the column specs of the given Object array
     * @return the {@link DataTable} representation of the Object array
     * @throws KNIMESparkException
     */
    public static DataTable convertResultToDataTable(final List<List<Serializable>> intermediateTypeRows,
        final DataTableSpec spec) throws KNIMESparkException {
        final KNIMEToIntermediateConverter[] converter = KNIMEToIntermediateConverterRegistry.getConverter(spec);
        final DataTableSpec resultSpec = getDataTableSpec(spec);
        final DataCell[][] rows = new DataCell[intermediateTypeRows.size()][spec.getNumColumns()];
        int rowIndex = 0;
        for(List<Serializable> row : intermediateTypeRows) {
            int columnIndex = 0;
            for(Serializable cellValue : row) {
                rows[rowIndex][columnIndex] = converter[columnIndex].convert(cellValue);
                columnIndex++;
            }
            rowIndex++;
        }
        return wrapAsDataTable(resultSpec, rows);
    }

    private static DataTable wrapAsDataTable(final DataTableSpec spec, final DataCell[][] rows) {
        return new DataTable() {
            @Override
            public RowIterator iterator() {
                return new RowIterator() {

                    private int currentRow = 0;

                    @Override
                    public DataRow next() {

                        currentRow++;
                        return new DataRow() {
                            private final int rowIndex = currentRow - 1;

                            private final RowKey m_rowKey = RowKey.createRowKey((long)rowIndex);

                            @Override
                            public Iterator<DataCell> iterator() {
                                return new Iterator<DataCell>() {
                                    private int currentCol = 0;

                                    @Override
                                    public boolean hasNext() {
                                        return currentCol < getNumCells();
                                    }

                                    @Override
                                    public DataCell next() {
                                        DataCell cell = getCell(currentCol);
                                        currentCol++;
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
                                return rows[rowIndex].length;
                            }

                            @Override
                            public RowKey getKey() {
                                return m_rowKey;
                            }

                            @Override
                            public DataCell getCell(final int index) {
                                return rows[rowIndex][index];
                            }
                        };
                    }

                    @Override
                    public boolean hasNext() {
                        return currentRow < rows.length;
                    }
                };
            }

            @Override
            public DataTableSpec getDataTableSpec() {
                return spec;
            }
        };
    }

    /**
     * @param knimeSpec {@link DataTableSpec} to convert
     * @return the {@link IntermediateSpec} representing the input {@link DataTableSpec}
     */
    public static IntermediateSpec toIntermediateSpec(final DataTableSpec knimeSpec) {
        final IntermediateField[] fields = new IntermediateField[knimeSpec.getNumColumns()];
        int idx = 0;
        for (final DataColumnSpec knimeColumnSpec : knimeSpec) {
            final IntermediateDataType intermediateType =
                    KNIMEToIntermediateConverterRegistry.get(knimeColumnSpec.getType()).getIntermediateDataType();
            fields[idx++] = new IntermediateField(knimeColumnSpec.getName(), intermediateType);
        }
        return new IntermediateSpec(fields);
    }
}
