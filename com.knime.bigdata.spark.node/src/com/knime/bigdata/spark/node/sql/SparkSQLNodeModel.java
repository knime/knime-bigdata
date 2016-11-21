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
 */
package com.knime.bigdata.spark.node.sql;

import java.util.List;

import org.knime.base.util.flowvariable.FlowVariableProvider;
import org.knime.base.util.flowvariable.FlowVariableResolver;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
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
import com.knime.bigdata.spark.core.job.EmptyJobInput;
import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.JobRunFactory;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;
import com.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import com.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark.core.util.SparkIDs;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class SparkSQLNodeModel extends SparkNodeModel implements FlowVariableProvider {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkSQLNodeModel.class);

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkSQLNodeModel.class.getCanonicalName();

    /** The functions Spark job id. */
    public static final String FUNCTIONS_JOB_ID = JOB_ID + "Functions";

    private final SparkSQLSettings m_settings = new SparkSQLSettings();

    /** Constructor. */
    public SparkSQLNodeModel() {
        super(new PortType[] {SparkDataPortObject.TYPE},
              new PortType[] {SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Spark RDD found.");
        }

        // We do not know the spec at this point
        return new PortObjectSpec[] { null };
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");

        final SparkDataPortObject inputRdd = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = inputRdd.getContextID();
        final String namedInputObject = inputRdd.getData().getID();
        final IntermediateSpec inputSchema = SparkDataTableUtil.toIntermediateSpec(inputRdd.getTableSpec());
        final String namedOutputObject = SparkIDs.createRDDID();
        final String query = FlowVariableResolver.parse(m_settings.getQuery(), this);
        final SparkSQLJobInput input = new SparkSQLJobInput(namedInputObject, inputSchema, namedOutputObject, query);

        LOGGER.debug("Executing SQL query: " + query);

        final JobOutput jobOutput = SparkContextUtil.getJobRunFactory(contextID, JOB_ID)
                .createRun(input)
                .run(contextID, exec);

        final DataTableSpec outputSpec = KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(namedOutputObject));
        final SparkDataTable resultTable = new SparkDataTable(contextID, namedOutputObject, outputSpec);
        final SparkDataPortObject sparkObject = new SparkDataPortObject(resultTable);

        return new PortObject[] { sparkObject };
    }

    /**
     * Runs spark functions job and return SQL function names.
     * @param contextID
     * @return List of Spark SQL functions
     * @throws KNIMESparkException
     */
    public static List<String> getSQLFunctions(final SparkContextID contextID) throws KNIMESparkException {
        final JobRunFactory<EmptyJobInput, SparkSQLFunctionsJobOutput> factory = SparkContextUtil.getJobRunFactory(contextID, FUNCTIONS_JOB_ID);
        final SparkSQLFunctionsJobOutput output = factory.createRun(new EmptyJobInput()).run(contextID);
        return output.getFunctions();
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadValidatedSettingsFrom(settings);
    }
}