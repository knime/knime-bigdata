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
package org.knime.bigdata.spark.node.ml.prediction.predictor.classification;

import java.util.ArrayList;
import java.util.List;

import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.model.MLModelHelper;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.port.model.ModelHelperRegistry;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;
import org.knime.bigdata.spark.core.port.model.ml.MLModel;
import org.knime.bigdata.spark.core.port.model.ml.MLModelType.Category;
import org.knime.bigdata.spark.core.port.model.ml.SparkMLModelPortObject;
import org.knime.bigdata.spark.core.port.model.ml.SparkMLModelPortObjectSpec;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.ml.prediction.MLPredictionUtils;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLPredictorClassificationNodeModel extends SparkNodeModel {

    /** The unique Spark job id for the ML classification predictor job */
    public static final String JOB_ID = "MLPredictorClassificationJob";

    private final MLPredictorClassificationNodeSettings m_settings = new MLPredictorClassificationNodeSettings();

    /** Constructor. */
    public MLPredictorClassificationNodeModel() {
        super(new PortType[]{SparkMLModelPortObject.PORT_TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("Input missing");
        }

        if (!(inSpecs[0] instanceof SparkMLModelPortObjectSpec)) {
            throw new InvalidSettingsException("Ingoing model must be a Spark ML model.");
        }

        final SparkMLModelPortObjectSpec modelSpec = (SparkMLModelPortObjectSpec)inSpecs[0];
        final SparkDataPortObjectSpec inputSparkData = (SparkDataPortObjectSpec)inSpecs[1];

        if (modelSpec.getModelType().getCategory() != Category.CLASSIFICATION) {
            throw new InvalidSettingsException(
                String.format("%s models are not supported by this node", modelSpec.getModelType().getUniqueName()));
        }

        MLPredictionUtils.checkFeatureColumns(modelSpec.getLearningColumnSpec(), inputSparkData.getTableSpec());

        final SparkVersion modelSparkVersion = modelSpec.getSparkVersion();
        final SparkVersion dataSparkVersion = SparkContextUtil.getSparkVersion(inputSparkData.getContextID());
        if (!modelSparkVersion.equals(dataSparkVersion)) {
            setWarningMessage(String.format(
                "Model was computed with Spark %s, but ingoing DataFrame belongs to a Spark %s context.\n"
                    + "Applying the model may cause errors.",
                modelSparkVersion.getLabel(), dataSparkVersion.getLabel()));
        }

        if (m_settings.getAppendClassProbabilityColumnsModel().getBooleanValue()) {
            // We do not know the spec at this point, because we do not know the
            // nominal values of the column to predict
            return new PortObjectSpec[]{null};
        } else {
            final DataTableSpec resultTableSpec =
                createSpecWithPredictionCol(inputSparkData.getTableSpec(), modelSpec.getTargetColumnSpec().get());
            return new PortObjectSpec[]{new SparkDataPortObjectSpec(inputSparkData.getContextID(), resultTableSpec)};
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {

        final SparkMLModelPortObject sparkMLModelPortObject = (SparkMLModelPortObject)inObjects[0];
        final SparkDataPortObject sparkDataPortObject = (SparkDataPortObject)inObjects[1];

        final MLModel mlModel = sparkMLModelPortObject.getModel();
        final SparkDataTable data = sparkDataPortObject.getData();

        final MLModelHelper mlModelHelper =
            ModelHelperRegistry.getMLModelHelper(mlModel.getModelName(), getSparkVersion(sparkDataPortObject));
        mlModelHelper.uploadModelToSparkIfNecessary(sparkDataPortObject.getContextID(), mlModel, exec);

        final String newNamedObject = SparkIDs.createSparkDataObjectID();

        final MLPredictorClassificationJobInput input =
            new MLPredictorClassificationJobInput(data.getID(),
                mlModel.getNamedModelId(),
                newNamedObject,
                determinePredictionColumnName(data.getTableSpec(), mlModel.getTargetColumnName().get()),
                m_settings.getAppendClassProbabilityColumnsModel().getBooleanValue(),
                determineProbabilityColumnSuffix());

        SparkContextUtil.getSimpleRunFactory(data.getContextID(), JOB_ID)
            .createRun(input)
            .run(data.getContextID());

        final DataTableSpec resultSpec;
        if (m_settings.getAppendClassProbabilityColumnsModel().getBooleanValue()) {
            resultSpec = createSpecWithProbabilityColumns(data.getTableSpec(), mlModel);
        } else {
            resultSpec = createSpecWithPredictionCol(data.getTableSpec(), mlModel.getTargetColumnSpec().get());
        }

        final SparkDataTable resultSparkData = new SparkDataTable(data.getContextID(), newNamedObject, resultSpec);

        return new PortObject[]{new SparkDataPortObject(resultSparkData)};
    }

    private DataTableSpec createSpecWithProbabilityColumns(final DataTableSpec tableSpec, final MLModel mlModel) {

        final DataTableSpec withPredictionCol =
            createSpecWithPredictionCol(tableSpec, mlModel.getTargetColumnSpec().get());

        final List<DataColumnSpec> columnsToAppend = new ArrayList<>();
        final String suffix = determineProbabilityColumnSuffix();

        final List<String> nominalValuesOfTargetCol =
            mlModel.getModelMetaData(MLMetaData.class).get().getNominalTargetValueMappings();
        for (String nominalValue : nominalValuesOfTargetCol) {
            final String probabilityColName =
                String.format("P (%s=%s)%s", mlModel.getTargetColumnName().get(), nominalValue, suffix);
            columnsToAppend.add(new DataColumnSpecCreator(probabilityColName, DoubleCell.TYPE).createSpec());
        }
        return new DataTableSpec(withPredictionCol, new DataTableSpec(columnsToAppend.toArray(new DataColumnSpec[0])));
    }

    private String determineProbabilityColumnSuffix() {
        if (m_settings.getAppendClassProbabilityColumnsModel().getBooleanValue()) {
            return m_settings.getProbabilityColumnSuffixModel().getStringValue();
        } else {
            return "";
        }
    }

    private String determinePredictionColumnName(final DataTableSpec inputSpec, final String targetColumnName) {
        final String predictionColumnName;
        if (m_settings.getOverwritePredictionColumnModel().getBooleanValue()) {
            predictionColumnName = m_settings.getPredictionColumnModel().getStringValue();
        } else {
            predictionColumnName = String.format("Prediction (%s)", targetColumnName);
        }

        return DataTableSpec.getUniqueColumnName(inputSpec, predictionColumnName);
    }

    private DataTableSpec createSpecWithPredictionCol(final DataTableSpec inputSpec,
        final DataColumnSpec targetColumnSpec) {
        final String predictionColName = determinePredictionColumnName(inputSpec, targetColumnSpec.getName());
        final DataColumnSpec predictionColSpec =
            new DataColumnSpecCreator(predictionColName, targetColumnSpec.getType()).createSpec();
        return new DataTableSpec(inputSpec, new DataTableSpec(predictionColSpec));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
    }
}
