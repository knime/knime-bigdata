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
 *   Created on 26.06.2015 by koetter
 */
package com.knime.bigdata.spark.node.io.table.reader;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collections;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.EmptyJobOutput;
import com.knime.bigdata.spark.core.job.JobWithFilesRunFactory;
import com.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;
import com.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import com.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverter;
import com.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Table2SparkNodeModel extends SparkSourceNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Table2SparkNodeModel.class);

    /**The unique Spark job id.*/
    public static final String JOB_ID = "Table2SparkJob";

    /** Constructor. */
    Table2SparkNodeModel() {
        super(new PortType[]{BufferedDataTable.TYPE}, new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("Please connect the input port");
        }
        DataTableSpec spec = (DataTableSpec)inSpecs[0];
        final SparkDataPortObjectSpec resultSpec = new SparkDataPortObjectSpec(getContextID(inSpecs), spec);
        return new PortObjectSpec[]{resultSpec};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        if (inData == null || inData.length < 1 || inData[0] == null) {
            throw new InvalidSettingsException("Please connect the input port");
        }
        //Check that the context is available before doing all the work
        final SparkContextID contextID = getContextID(inData);
        ensureContextIsOpen(contextID);

        exec.setMessage("Converting data table...");
        final ExecutionMonitor subExec = exec.createSubProgress(0.9);
        final BufferedDataTable table = (BufferedDataTable)inData[0];
        final File convertedInputTable = writeBufferedDataTable(table, subExec);

        exec.setMessage("Importing data into Spark...");
        final SparkDataTable resultTable = new SparkDataTable(contextID, table.getDataTableSpec());
        executeSparkJob(contextID, exec, convertedInputTable, resultTable);

        exec.setProgress(1, "Spark data object created");
        return new PortObject[]{new SparkDataPortObject(resultTable)};
    }

    private void executeSparkJob(final SparkContextID contextID, final ExecutionContext exec,
        final File serializedTableFile, final SparkDataTable resultTable) throws KNIMESparkException, CanceledExecutionException {

        final Table2SparkJobInput input = Table2SparkJobInput.create(resultTable.getID(),
            SparkDataTableUtil.toIntermediateSpec(resultTable.getTableSpec()));

        final JobWithFilesRunFactory<Table2SparkJobInput, EmptyJobOutput> execProvider =
            SparkContextUtil.getJobWithFilesRunFactory(contextID, JOB_ID);

        execProvider.createRun(input, Collections.singletonList(serializedTableFile)).run(contextID,
            exec);
    }

    private File writeBufferedDataTable(final BufferedDataTable inputTable, final ExecutionMonitor exec)
        throws IOException, KNIMESparkException, CanceledExecutionException {

        final KNIMEToIntermediateConverter[] converters = KNIMEToIntermediateConverterRegistry.getConverter(inputTable.getDataTableSpec());

        final File outFile = File.createTempFile("knime-table2spark", ".tmp");
        addFileToDeleteAfterExecute(outFile);

        LOGGER.debugWithFormat("Serializing data table to file %s", outFile.getAbsolutePath());

        try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)))) {
            final long rowCount = inputTable.size();
            final DataTableSpec spec = inputTable.getSpec();

            out.writeLong(rowCount);
            out.writeInt(spec.getNumColumns());
            long rowIdx = 0;
            for (final DataRow row : inputTable) {

                if (rowIdx % 10 == 0) {
                    exec.checkCanceled();
                    exec.setProgress(rowIdx / (double)rowCount, "Processing row " + rowIdx + " of " + rowCount);
                }

                int colIdx = 0;
                for (final DataCell cell : row) {
                    out.writeObject(converters[colIdx].convert(cell));
                    colIdx++;
                }
                rowIdx++;
            }
        }

        exec.setProgress(1);

        return outFile;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
    }

}
