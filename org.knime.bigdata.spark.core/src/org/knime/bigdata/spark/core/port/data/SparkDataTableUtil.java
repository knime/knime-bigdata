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
 *   Created on 28.05.2015 by koetter
 */
package org.knime.bigdata.spark.core.port.data;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.knime.bigdata.spark.core.context.SparkContextConstants;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobRunFactory;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverter;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterParameter;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowIterator;
import org.knime.core.data.RowKey;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

/**
 * Class with utility functions for {@link SparkDataTable}.
 *
 * @author Tobias Koetter, KNIME GmbH
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public final class SparkDataTableUtil {


    /**
     * Converts the given KNIME spec into an intermediate spec and back again. The resulting KNIME spec may differ from
     * the given one insofar as default converters may be used to convert certain KNIME types. For columns where the
     * given and resulting KNIME types are identical, all column attributes are passed through to the resulting column
     * spec. Otherwise, only name and properties passed through.
     *
     * @param inputSpec A {@link DataTableSpec} to convert.
     * @return a {@link DataTableSpec} that results from converting the given KNIME spec into an intermediate spec and
     *         back again
     * @deprecated This implementation uses the first available type converter. See
     *             {@link KNIMEToIntermediateConverterRegistry#convertSpec(IntermediateSpec)}.
     */
    @Deprecated
    public static DataTableSpec getTransferredSparkDataTableSpec(final DataTableSpec inputSpec) {
        final IntermediateSpec intermediateSpec = SparkDataTableUtil.toIntermediateSpec(inputSpec);
        final DataTableSpec outputSpec = KNIMEToIntermediateConverterRegistry.convertSpec(intermediateSpec);
        final DataColumnSpec[] outputColumns = new DataColumnSpec[inputSpec.getNumColumns()];

        for (int i = 0; i < inputSpec.getNumColumns(); i++) {
            final DataColumnSpec inputCol = inputSpec.getColumnSpec(i);
            final DataType outputType = outputSpec.getColumnSpec(i).getType();
            final DataColumnSpecCreator creator;

            if (inputCol.getType().equals(outputType)) {
                creator = new DataColumnSpecCreator(inputCol);
            } else {
                creator = new DataColumnSpecCreator(inputCol.getName(), outputType);
                creator.setProperties(inputCol.getProperties());
            }

            outputColumns[i] = creator.createSpec();
        }

        return new DataTableSpec(outputColumns);
    }


    /**
     * Retrieves all rows of the given {@link SparkDataTable} and converts them into a KNIME data table.
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
     * Retrieves the given number of rows from the given {@link SparkDataTable} and converts them into a KNIME data
     * table.
     *
     * @param exec An {@link ExecutionMonitor} to check for cancellation. Can be <code>null</code>.
     * @param data the Spark data table.
     * @param cacheNoRows The number of rows to retrieve. If set to -1 then all rows are retrieved
     * @return a local KNIME data table.
     * @throws CanceledExecutionException If retrieval of the {@link SparkDataTable} was canceled via the given {@link ExecutionMonitor}.
     * @throws KNIMESparkException If something went wrong during retrieval of the {@link SparkDataTable} (e.g. network errors)
     */
    public static DataTable getDataTable(final ExecutionMonitor exec, final SparkDataTable data, final int cacheNoRows) throws CanceledExecutionException, KNIMESparkException {

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
                    return getTransferredSparkDataTableSpec(data.getTableSpec());
                }
            };
        }
        if (exec != null) {
            exec.checkCanceled();
        }
        return fetchDataTable(data, cacheNoRows, exec);
    }

    private static DataTable fetchDataTable(final SparkDataTable data, final int noOfRows, final ExecutionMonitor exec)
            throws KNIMESparkException, CanceledExecutionException {

        final IntermediateSpec intermediateSpec = getIntermediateSpec(data);
        final FetchRowsJobInput input = FetchRowsJobInput.create(noOfRows, data.getID(), intermediateSpec);
        final JobRunFactory<FetchRowsJobInput, FetchRowsJobOutput> execProvider =
            SparkContextUtil.getJobRunFactory(data.getContextID(), SparkContextConstants.FETCH_ROWS_JOB_ID);
        final FetchRowsJobOutput output = execProvider.createRun(input).run(data.getContextID(), exec);
        return convertResultToDataTable(output.getRows(), data);
    }


    private static DataTable convertResultToDataTable(final List<List<Serializable>> intermediateTypeRows,
        final SparkDataTable sparkDataTabe) throws KNIMESparkException {

        final DataTableSpec spec = getTransferredSparkDataTableSpec(sparkDataTabe.getTableSpec());
        final KNIMEToIntermediateConverter[] converter =
            KNIMEToIntermediateConverterRegistry.getConverters(spec);
        final KNIMEToIntermediateConverterParameter converterParameter =
            SparkContextUtil.getConverterParameter(sparkDataTabe.getContextID());
        final DataCell[][] rows = new DataCell[intermediateTypeRows.size()][spec.getNumColumns()];

        int rowIndex = 0;
        for (List<Serializable> row : intermediateTypeRows) {
            int columnIndex = 0;
            for (Serializable cellValue : row) {
                rows[rowIndex][columnIndex] = converter[columnIndex].convert(cellValue, converterParameter);
                columnIndex++;
            }
            rowIndex++;
        }
        return wrapAsDataTable(spec, rows);
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
     * Creates an intermediate spec for the given {@link SparkDataTable}.
     *
     * @param sparkDataTable The {@link SparkDataTable} for which to get an intermediate spec.
     * @return the {@link IntermediateSpec} derived from the spec of the given {@link SparkDataTable}.
     */
    public static IntermediateSpec getIntermediateSpec(final SparkDataTable sparkDataTable) {
        return toIntermediateSpec(sparkDataTable.getTableSpec());
    }

    /**
     * Creates an intermediate spec for the given {@link DataTableSpec}.
     *
     * @param spec The {@link DataTableSpec} for which to get an intermediate spec.
     * @return the {@link IntermediateSpec} derived from the spec of the given {@link DataTableSpec}.
     */
    public static IntermediateSpec toIntermediateSpec(final DataTableSpec spec) {
        final IntermediateField[] fields = new IntermediateField[spec.getNumColumns()];
        int idx = 0;
        for (final DataColumnSpec knimeColumnSpec : spec) {
            final IntermediateDataType intermediateType =
                    KNIMEToIntermediateConverterRegistry.get(knimeColumnSpec.getType()).getIntermediateDataType();
            fields[idx++] = new IntermediateField(knimeColumnSpec.getName(), intermediateType);
        }
        return new IntermediateSpec(fields);

    }
}
