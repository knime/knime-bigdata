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
package org.knime.bigdata.spark.node.io.table.writer;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.core.data.RowIterator;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortObjectInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.core.node.streamable.StreamableOperator;

/**
 * Base class for streamable operators for the {@link Spark2TableNodeModel}.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public abstract class AbstractSpark2TableStreamableOperator extends StreamableOperator {

    /**
     * {@inheritDoc}
     */
    @Override
    public void runFinal(final PortInput[] inputs, final PortOutput[] outputs, final ExecutionContext exec)
        throws Exception {

        final RowOutput rowOutput = (RowOutput)outputs[0];
        final SparkDataTable sparkDataTable =
            ((SparkDataPortObject)((PortObjectInput)inputs[0]).getPortObject()).getData();

        runWithRowOutput(sparkDataTable, rowOutput, exec);
    }

    /**
     * Transfers rows from the row iterator returned by {@link #getRowIterator(SparkDataTable, ExecutionContext)} to the
     * given {@link RowOutput}.
     *
     * @param sparkDataTable The Spark data table providing the input data.
     * @param rowOutput The row output to write rows to.
     * @param exec Execution context for progress reporting.
     * @throws InterruptedException when the current thread is interrupted while it executes this method.
     * @throws CanceledExecutionException when the execution is canceled via the provided execution context.
     * @throws KNIMESparkException when something goes wrong while downloading data from Spark.
     */
    public void runWithRowOutput(final SparkDataTable sparkDataTable, final RowOutput rowOutput,
        final ExecutionContext exec) throws InterruptedException, CanceledExecutionException, KNIMESparkException {

        // this iterator is either sourced by the rows fetched from a remote Spark context (normal mode),
        // or by an iterator that feeds from a RDD/Dataset partition inside Spark (KNOSP mode).
        final RowIterator iterator = getRowIterator(sparkDataTable, exec);

        // now that we have the iterator push the rows out
        while (iterator.hasNext()) {
            rowOutput.push(iterator.next());
        }

        // finally close the row output
        rowOutput.close();
    }

    /**
     * Called by {@link #runWithRowOutput(SparkDataTable, RowOutput, ExecutionContext)} to obtain a row iterator to read
     * rows from.
     *
     * @param exec Execution context for progress reporting.
     * @param sparkDataTable The Spark data table providing the input data.
     * @return a {@link RowIterator} that provides rows from the Spark data table.
     * @throws InterruptedException when the current thread is interrupted while it executes this method.
     * @throws CanceledExecutionException when the execution is canceled via the provided execution context.
     * @throws KNIMESparkException when something goes wrong while downloading data from Spark.
     */
    protected abstract RowIterator getRowIterator(final SparkDataTable sparkDataTable, final ExecutionContext exec)
        throws InterruptedException, CanceledExecutionException, KNIMESparkException;

}
