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
 *   Created on Aug 22, 2016 by bjoern
 */
package com.knime.bigdata.spark.node.io.table.reader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.StreamableOperator;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.EmptyJobOutput;
import com.knime.bigdata.spark.core.job.JobWithFilesRunFactory;
import com.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataTableUtil;

/**
 * A {@link StreamableOperator} implementation for the {@link Table2SparkNodeModel}. Rows consumed by this stremable
 * operator are serialized into a file and uploaded to a remote Spark context.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class Table2SparkStreamableOperator extends AbstractTable2SparkStreamableOperator {

    private final SparkContextID m_contextID;

    private final File m_tmpFile;

    /**
     * Creates a new streamable operator that writes the ingoing KNIME data table to a temporary file, which is then
     * transfered that to the given remote Spark context.
     *
     * @param contextID
     * @param tempFile
     */
    public Table2SparkStreamableOperator(final SparkContextID contextID, final File tempFile) {
        super();

        m_contextID = contextID;
        m_tmpFile = tempFile;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void runWithRowInput(final RowInput rowInput, final ExecutionContext exec) throws FileNotFoundException,
        IOException, InterruptedException, CanceledExecutionException, KNIMESparkException {

        super.runWithRowInput(rowInput, exec);

        // addendum to the default runWithRowInput() method because we need to execute a Spark job now to upload the data
        executeSparkJob(exec, rowInput.getDataTableSpec());
    }

    private void executeSparkJob(final ExecutionMonitor exec, final DataTableSpec spec)
        throws KNIMESparkException, CanceledExecutionException {

        SparkSourceNodeModel.ensureContextIsOpen(m_contextID);

        exec.setMessage("Importing data into Spark...");

        final Table2SparkJobInput input = Table2SparkJobInput.create(getNamedOutputObjectId(),
            SparkDataTableUtil.toIntermediateSpec(spec));

        final JobWithFilesRunFactory<Table2SparkJobInput, EmptyJobOutput> execProvider =
            SparkContextUtil.getJobWithFilesRunFactory(m_contextID, Table2SparkNodeModel.JOB_ID);

        execProvider.createRun(input, Collections.singletonList(m_tmpFile)).run(m_contextID, exec);
        exec.setProgress(1, "Data successfully imported into Spark");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BlockingQueue<Serializable[]> createQueue(final RowInput rowInput) throws IOException {

        long noOfRows = -1;
        if (rowInput instanceof DataTableRowInput) {
            noOfRows = ((DataTableRowInput)rowInput).getRowCount();
        }

        return new FileSerializingQueue(m_tmpFile, rowInput.getDataTableSpec().getNumColumns(), noOfRows);
    }
}
