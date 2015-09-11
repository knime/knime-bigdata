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
package com.knime.bigdata.spark.node.io.table.writer;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.client.UploadUtil;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.converter.SparkTypeConverter;
import com.knime.bigdata.spark.util.converter.SparkTypeRegistry;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Table2SparkNodeModel extends SparkNodeModel {

    /** Constructor. */
    Table2SparkNodeModel() {
        super(new PortType[]{BufferedDataTable.TYPE}, new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("Please connect the input port");
        }
        DataTableSpec spec = (DataTableSpec)inSpecs[0];
        final SparkDataPortObjectSpec resultSpec = new SparkDataPortObjectSpec(getContext(), spec);
        return new PortObjectSpec[]{resultSpec};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        if (inData == null || inData.length != 1 || inData[0] == null) {
            throw new InvalidSettingsException("Please connect the input port");
        }
        exec.setMessage("Converting data table...");
        final ExecutionMonitor subExec = exec.createSubProgress(0.7);
        final BufferedDataTable table = (BufferedDataTable)inData[0];
        final int rowCount = table.getRowCount();
        final DataTableSpec spec = table.getSpec();
        final SparkTypeConverter<?, ?>[] converter = SparkTypeRegistry.getConverter(spec);
        //        //extract primitive Java Types
        //        final Class<?>[] primitiveTypes = new Class<?>[converter.length];
        //        for (int colIx = 0; colIx < converter.length; colIx++) {
        //            primitiveTypes[colIx] = converter[colIx].getPrimitiveType();
        //        }
        final Object[][] data = new Object[rowCount][converter.length];
        int rowIdx = 0;
        for (final DataRow row : table) {
            subExec.setProgress(rowIdx / (double)rowCount, "Processing row " + rowIdx + " of " + rowCount);
            exec.checkCanceled();
            int colIdx = 0;
            for (final DataCell cell : row) {
                data[rowIdx][colIdx] = converter[colIdx].convert(cell);
                colIdx++;
            }
            rowIdx++;
        }

        exec.setMessage("Sending data to Spark...");
        final SparkDataTable resultTable = new SparkDataTable(getContext(), table.getDataTableSpec());
        executeSparkJob(exec, data, resultTable);
        exec.setProgress(1, "Spark data object created");
        return new PortObject[]{new SparkDataPortObject(resultTable)};
    }

    private KNIMESparkContext getContext() throws InvalidSettingsException {
        try {
            return KnimeContext.getSparkContext();
        } catch (GenericKnimeSparkException e) {
            throw new InvalidSettingsException(e.getMessage());
        }
    }

    /**
     * run the job on the server: send table as object array to server and create RDD from it
     *
     * @param exec
     * @throws Exception
     */
    private void executeSparkJob(final ExecutionContext exec, final Object[][] data, final SparkDataTable resultTable)
        throws Exception {

        final UploadUtil util = new UploadUtil(resultTable.getContext(), data, "data-table");
        util.upload();
        try {
            final String params = paramDef(util.getServerFileName(), resultTable.getID());
            exec.checkCanceled();
            JobControler.startJobAndWaitForResult(resultTable.getContext(),
                ImportKNIMETableJob.class.getCanonicalName(), params, exec);
        } finally {
            util.cleanup();
        }
    }

    /**
     *
     * @param aDataFileName - absolute path on server to file with data (upload the data before calling this)
     * @param aResultTableName
     * @return JSon string with job parameters
     * @throws GenericKnimeSparkException
     */
    public static String paramDef(final String aDataFileName, final String aResultTableName)
        throws GenericKnimeSparkException {

        if (aDataFileName == null) {
            throw new NullPointerException("Data file name must not be null!");
        }
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            new Object[]{KnimeSparkJob.PARAM_INPUT_TABLE, JobConfig.encodeToBase64(aDataFileName)},
            ParameterConstants.PARAM_OUTPUT, new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, aResultTableName}});
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
