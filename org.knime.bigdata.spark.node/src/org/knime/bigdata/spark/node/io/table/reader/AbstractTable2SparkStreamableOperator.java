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
 *   Created on Sep 22, 2017 by bjoern
 */
package org.knime.bigdata.spark.node.io.table.reader;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverter;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterParameter;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortObjectInput;
import org.knime.core.node.streamable.PortObjectOutput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.StreamableOperator;

/**
 * Base class for streamable operators for the {@link Table2SparkNodeModel}.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public abstract class AbstractTable2SparkStreamableOperator extends StreamableOperator {

    private final String m_namedOutputObjectId;

    /**
     * Constructor.
     */
    protected AbstractTable2SparkStreamableOperator() {
        m_namedOutputObjectId = SparkIDs.createSparkDataObjectID();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void runFinal(final PortInput[] inputs, final PortOutput[] outputs, final ExecutionContext exec)
        throws Exception {

        final RowInput rowInput = (RowInput)inputs[0];
        final SparkContextPortObject contextPortObject =
            (SparkContextPortObject)((PortObjectInput)inputs[1]).getPortObject();
        final KNIMEToIntermediateConverterParameter converterParameter =
            SparkContextUtil.getConverterParameter(contextPortObject.getContextID());

        runWithRowInput(rowInput, exec, converterParameter);

        // if you change this, you also need to change the behavior in Table2SparkNodeModel#configureInternal()
        // and Table2SparkNodeModel#executeInternal()
        final DataTableSpec outputSpec = Table2SparkNodeModel.createSparkDataTableSpec(rowInput.getDataTableSpec());

        final SparkDataPortObject outPortObject =
            new SparkDataPortObject(createSparkDataTable(contextPortObject.getContextID(), outputSpec));

        ((PortObjectOutput)outputs[0]).setPortObject(outPortObject);
    }

    /**
     * Creates the output {@link SparkDataTable} instance.
     *
     * @param contextID The ID of context the Spark data table lives in.
     * @param spec The {@link DataTableSpec} of the Spark data table.
     * @return Output {@link SparkDataTable} instance.
     */
    protected abstract SparkDataTable createSparkDataTable(final SparkContextID contextID, final DataTableSpec spec);

    /**
     * Used by {@link #runWithRowInput(RowInput, ExecutionContext, KNIMEToIntermediateConverterParameter)} to obtain a
     * queue for writing rows.
     *
     * @param rowInput The row input from which rows are assumed to come.
     * @return A queue to write rows to.
     * @throws IOException when something goes wrong while allocating the queue.
     */
    protected abstract BlockingQueue<Serializable[]> createQueue(final RowInput rowInput) throws IOException;

    /**
     * Reads all rows from the row input, converts them to the intermediate type domain and pushes those values into the
     * queue obtained by {@link #createQueue(RowInput)}.
     *
     * @param exec Execution context for progress reporting.
     * @param rowInput The row input to read rows from.
     * @param converterParameter Context specific converter parameters.
     * @throws IOException when something goes wrong while allocating the queue.
     * @throws InterruptedException when the current thread is interrupted while it executes this method.
     * @throws CanceledExecutionException when the execution is canceled via the provided execution context.
     * @throws KNIMESparkException when something goes wrong while uploading the data to Spark.
     */
    public void runWithRowInput(final RowInput rowInput, final ExecutionContext exec,
        final KNIMEToIntermediateConverterParameter converterParameter)
        throws IOException, InterruptedException, CanceledExecutionException, KNIMESparkException {

        exec.checkCanceled();
        final ExecutionMonitor subExec = exec.createSubProgress(0.5);
        final BlockingQueue<Serializable[]> queue = createQueue(rowInput);

        long noOfRows = -1;
        if (rowInput instanceof DataTableRowInput) {
            noOfRows = ((DataTableRowInput)rowInput).getRowCount();
        }

        // transfer all rows from rowInput to m_queue
        convertRowsAndTransferToQueue(rowInput, noOfRows, subExec, queue, converterParameter);
    }

    private void convertRowsAndTransferToQueue(final RowInput rowInput, final long noOfRows,
        final ExecutionMonitor exec, final BlockingQueue<Serializable[]> queue,
        final KNIMEToIntermediateConverterParameter converterParameter)
        throws InterruptedException, CanceledExecutionException {

        KNIMEToIntermediateConverter[] m_converters = KNIMEToIntermediateConverterRegistry
            .getConverters(rowInput.getDataTableSpec());

        final int noColums = rowInput.getDataTableSpec().getNumColumns();

        exec.setMessage("Processing rows from input table...");
        DataRow row;
        long rowsWritten = 0;
        while ((row = rowInput.poll()) != null) {
            int colIdx = 0;
            final Serializable[] array = new Serializable[noColums];
            for (final DataCell cell : row) {
                array[colIdx] = m_converters[colIdx].convert(cell, converterParameter);
                colIdx++;
            }
            queue.put(array);
            rowsWritten++;

            if (rowsWritten % 500 == 0) {
                exec.checkCanceled();

                if (noOfRows != -1) {
                    exec.setProgress(rowsWritten / (double)noOfRows,
                        "Processing row " + rowsWritten + " of " + noOfRows);
                }
            }
        }

        // this indicates the end of the table
        queue.put(new Serializable[0]);
    }

    /**
     * @return the ID of the named object under which the input table has been imported (when in normal mode).
     * @throws IllegalArgumentException when invoking this method in KNIME-on-Spark (KNOSP) mode.
     */
    public String getNamedOutputObjectId() {
        return m_namedOutputObjectId;
    }
}
