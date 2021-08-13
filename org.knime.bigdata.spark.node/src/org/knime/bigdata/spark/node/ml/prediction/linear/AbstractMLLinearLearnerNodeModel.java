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
 */
package org.knime.bigdata.spark.node.ml.prediction.linear;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.bigdata.spark.core.node.SparkMLModelLearnerNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.model.ml.MLModel;
import org.knime.bigdata.spark.core.port.model.ml.MLModelType;
import org.knime.bigdata.spark.core.port.model.ml.SparkMLModelPortObject;
import org.knime.bigdata.spark.core.port.model.ml.SparkMLModelPortObjectSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataTableSpecCreator;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Abstract superclass of all linear Spark ML model learner nodes.
 *
 * @author Sascha Wolke, KNIME GmbH
 * @param <I> The {@link JobInput}
 * @param <T> The {@link MLlibNodeSettings}
 */
public abstract class AbstractMLLinearLearnerNodeModel<I extends MLLinearLearnerJobInput, T extends LinearLearnerSettings>
    extends SparkMLModelLearnerNodeModel<I, T> {

    /**
     * @param modelType
     * @param jobId
     * @param settings
     */
    protected AbstractMLLinearLearnerNodeModel(final MLModelType modelType, final String jobId, final T settings) {
        super(new PortType[]{SparkDataPortObject.TYPE}, //
            new PortType[]{SparkMLModelPortObject.PORT_TYPE, BufferedDataTable.TYPE, BufferedDataTable.TYPE}, //
            modelType, //
            jobId, //
            settings);
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkMLModelPortObjectSpec modelPortSpec = (SparkMLModelPortObjectSpec)super.configureInternal(inSpecs)[0];
        return new PortObjectSpec[]{ //
            modelPortSpec, //
            createCoefficientsAndInterceptTableSpec(), //
            createModelStatisticsTableSpec()};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkMLModelPortObject modelPortObject = learnModel(inData, exec);
        final boolean fitIntercept = getSettings().fitIntercept();
        final MLModel model = modelPortObject.getModel();
        final MLLinearLearnerMetaData metaData = model.getModelMetaData(MLLinearLearnerMetaData.class).orElseThrow();
        return new PortObject[]{ //
            modelPortObject, //
            createCoefficientsAndInterceptTable(exec, metaData, fitIntercept), //
            createModelStatisticsTable(exec, metaData)};

    }

    private static DataTableSpec createCoefficientsAndInterceptTableSpec() {
        return new DataTableSpecCreator() //
            .addColumns(new DataColumnSpecCreator("Variable", StringCell.TYPE).createSpec()) //
            .addColumns(new DataColumnSpecCreator("Coeff.", DoubleCell.TYPE).createSpec()) //
            .addColumns(new DataColumnSpecCreator("Std. Err.", DoubleCell.TYPE).createSpec()) //
            .addColumns(new DataColumnSpecCreator("t-value", DoubleCell.TYPE).createSpec()) //
            .addColumns(new DataColumnSpecCreator("P>|t|", DoubleCell.TYPE).createSpec()) //
            .createSpec();
    }

    private static BufferedDataTable createCoefficientsAndInterceptTable(final ExecutionContext exec,
        final MLLinearLearnerMetaData metaData, final boolean fitIntercept) {

        final String[] featureValues = metaData.getNominalFeatureValues().toArray(new String[0]);
        final double[] coefficients = metaData.getCoefficients();
        final double[] coefficientsStdErr = metaData.getCoefficientStandardErrors();
        final double[] pValues = metaData.getPValues();
        final double[] tValues = metaData.getTValues();

        final BufferedDataContainer dataContainer = exec.createDataContainer(createCoefficientsAndInterceptTableSpec());
        for (int i = 0; i < coefficients.length; i++) {
            if (coefficientsStdErr == null) {
                dataContainer.addRowToTable(new DefaultRow( //
                    RowKey.createRowKey(i + 1L), //
                    new StringCell(featureValues[i]), //
                    new DoubleCell(coefficients[i]), //
                    DataType.getMissingCell(), //
                    DataType.getMissingCell(), //
                    DataType.getMissingCell()));
            } else {
                dataContainer.addRowToTable(new DefaultRow( //
                    RowKey.createRowKey(i + 1L), //
                    new StringCell(featureValues[i]), //
                    new DoubleCell(coefficients[i]), //
                    new DoubleCell(coefficientsStdErr[i]), //
                    new DoubleCell(pValues[i]), //
                    new DoubleCell(tValues[i])));
            }
        }

        if (fitIntercept) { // add Intercept row
            final int i = coefficients.length;
            final double intercept = metaData.getIntercept();

            if (coefficientsStdErr == null) {
                dataContainer.addRowToTable(new DefaultRow( //
                    RowKey.createRowKey(i + 1L), //
                    new StringCell("Intercept"), //
                    new DoubleCell(intercept), //
                    DataType.getMissingCell(), //
                    DataType.getMissingCell(), //
                    DataType.getMissingCell()));
            } else {
                dataContainer.addRowToTable(new DefaultRow( //
                    RowKey.createRowKey(i + 1L), //
                    new StringCell("Intercept"), //
                    new DoubleCell(intercept), //
                    new DoubleCell(coefficientsStdErr[i]), //
                    new DoubleCell(pValues[i]), //
                    new DoubleCell(tValues[i])));
            }
        }

        dataContainer.close();

        return dataContainer.getTable();
    }

    private static DataTableSpec createModelStatisticsTableSpec() {
        return new DataTableSpecCreator() //
            .addColumns(new DataColumnSpecCreator("R^2", DoubleCell.TYPE).createSpec()) //
            .addColumns(new DataColumnSpecCreator("Adjusted R^2", DoubleCell.TYPE).createSpec()) //
            .addColumns(new DataColumnSpecCreator("Explained Variance", DoubleCell.TYPE).createSpec()) //
            .addColumns(new DataColumnSpecCreator("Mean Absolute Error", DoubleCell.TYPE).createSpec()) //
            .addColumns(new DataColumnSpecCreator("Mean Squared Error", DoubleCell.TYPE).createSpec()) //
            .addColumns(new DataColumnSpecCreator("Root Mean Squared Error", DoubleCell.TYPE).createSpec()) //
            .createSpec();
    }

    private static BufferedDataTable createModelStatisticsTable(final ExecutionContext exec,
        final MLLinearLearnerMetaData metaData) {

        final BufferedDataContainer dataContainer = exec.createDataContainer(createModelStatisticsTableSpec());
        dataContainer.addRowToTable(new DefaultRow( //
            RowKey.createRowKey(1L), //
            new DoubleCell(metaData.getRSquared()), //
            new DoubleCell(metaData.getRSquaredAdjusted()), //
            new DoubleCell(metaData.getExplainedVariance()), //
            new DoubleCell(metaData.getMeanAbsoluteError()), //
            new DoubleCell(metaData.getMeanSquaredError()), //
            new DoubleCell(metaData.getRootMeanSquaredError())));
        dataContainer.close();

        return dataContainer.getTable();
    }
}