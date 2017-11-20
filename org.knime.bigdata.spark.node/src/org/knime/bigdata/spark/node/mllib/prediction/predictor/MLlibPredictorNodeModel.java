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
 *   Created on 12.02.2015 by koetter
 */
package org.knime.bigdata.spark.node.mllib.prediction.predictor;

import java.io.File;
import java.util.Collections;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import org.knime.bigdata.spark.core.port.model.SparkModel;
import org.knime.bigdata.spark.core.port.model.SparkModelPortObject;
import org.knime.bigdata.spark.core.port.model.SparkModelPortObjectSpec;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibPredictorNodeModel extends SparkNodeModel {

    private final SettingsModelString m_colName = createColumnNameModel();

    /**The unique Spark job id.*/
    public static final String JOB_ID =  "MLlibPredictorJob";

    /**Constructor.*/
    public MLlibPredictorNodeModel() {
        super(new PortType[]{SparkModelPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelString createColumnNameModel() {
        return new SettingsModelString("columnName", "Prediction");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("Input missing");
        }

        final SparkModelPortObjectSpec inputModel = (SparkModelPortObjectSpec)inSpecs[0];
        final SparkDataPortObjectSpec inputRDD = (SparkDataPortObjectSpec)inSpecs[1];

        final SparkVersion modelSparkVersion = inputModel.getSparkVersion();
        final SparkVersion rddSparkVersion = SparkContextUtil.getSparkVersion(inputRDD.getContextID());
        if (!modelSparkVersion.equals(rddSparkVersion)) {
            setWarningMessage(
                String.format("Spark MLlib model was computed with Spark %s, but ingoing RDD belongs to a Spark %s context.\n"
                    + "Applying the model may therefore cause errors.",
                    modelSparkVersion.getLabel(), rddSparkVersion.getLabel()));
        }

        final DataTableSpec resultTableSpec = createSpec(inputRDD.getTableSpec());
        return new PortObjectSpec[]{
            new SparkDataPortObjectSpec(inputRDD.getContextID(), resultTableSpec)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkModel model = ((SparkModelPortObject)inObjects[0]).getModel();
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[1];
        exec.checkCanceled();
        exec.setMessage("Starting Spark predictor");
        final DataTableSpec inputSpec = data.getTableSpec();
        final Integer[] colIdxs = model.getLearningColumnIndices(inputSpec);
        final DataTableSpec resultSpec = createSpec(inputSpec);
        final IntermediateSpec resultsInterSpec = SparkDataTableUtil.toIntermediateSpec(resultSpec);
        final String aOutputTableName = SparkIDs.createSparkDataObjectID();
        final SparkDataTable resultRDD =
            new SparkDataTable(data.getContextID(), aOutputTableName, resultSpec);
        final PredictionJobInput jobInput = new PredictionJobInput(data.getTableName(), colIdxs, resultRDD.getID(), resultsInterSpec);

        final File modelFile = jobInput.writeModelIntoTemporaryFile(model.getModel());
        addFileToDeleteAfterExecute(modelFile);

        SparkContextUtil.getJobWithFilesRunFactory(data.getContextID(), JOB_ID)
            .createRun(jobInput, Collections.singletonList(modelFile))
            .run(data.getContextID(), exec);

        exec.setMessage("Spark prediction done.");
        return new PortObject[]{new SparkDataPortObject(resultRDD)};
    }

    /**
     * @param inputSpec the input data spec
     * @return the result spec
     */
    public DataTableSpec createSpec(final DataTableSpec inputSpec) {
        //TK_TODO: Here we need to create the right spec
        return createSpec(inputSpec, m_colName.getStringValue());
    }

    /**
     * Creates the typical cluster assignment spec with the cluster column as last column of type string.
     * @param inputSpec the input data spec
     * @param resultColName the name of the result column
     * @return the result spec
     */
    public static DataTableSpec createSpec(final DataTableSpec inputSpec, final String resultColName) {
        //TK_TODO: return Cluster instead of cluster
        final String clusterColName = DataTableSpec.getUniqueColumnName(inputSpec, resultColName);
        final DataColumnSpecCreator creator = new DataColumnSpecCreator(clusterColName, DoubleCell.TYPE);
        final DataColumnSpec labelColSpec = creator.createSpec();
        return new DataTableSpec(inputSpec, new DataTableSpec(labelColSpec));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_colName.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_colName.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_colName.loadSettingsFrom(settings);
    }
}
