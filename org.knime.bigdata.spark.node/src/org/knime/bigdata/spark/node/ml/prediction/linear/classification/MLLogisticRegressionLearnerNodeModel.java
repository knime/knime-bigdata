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
package org.knime.bigdata.spark.node.ml.prediction.linear.classification;

import java.util.ArrayList;
import java.util.List;

import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.node.SparkMLModelLearnerNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.model.ml.MLModel;
import org.knime.bigdata.spark.core.port.model.ml.MLModelType;
import org.knime.bigdata.spark.core.port.model.ml.MLModelType.Category;
import org.knime.bigdata.spark.core.port.model.ml.SparkMLModelPortObject;
import org.knime.bigdata.spark.core.port.model.ml.SparkMLModelPortObjectSpec;
import org.knime.bigdata.spark.node.ml.prediction.linear.LinearLearnerMode;
import org.knime.bigdata.spark.node.ml.prediction.linear.LinearLearnerSettings;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataTableSpecCreator;
import org.knime.core.data.MissingCell;
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
 * ML-based Spark logistic regression learner model.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class MLLogisticRegressionLearnerNodeModel
    extends SparkMLModelLearnerNodeModel<MLLogisticRegressionLearnerJobInput, LinearLearnerSettings> {

    /** Unique model name. */
    public static final String MODEL_NAME = "MLLogisticRegression";

    /** The model type. */
    public static final MLModelType MODEL_TYPE = MLModelType.getOrCreate(Category.CLASSIFICATION, MODEL_NAME);

    /** Unique job id. */
    public static final String JOB_ID = "MLLogisticRegressionLearnerJob";

    /**
     * Constructor.
     */
    public MLLogisticRegressionLearnerNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE}, //
            new PortType[]{SparkMLModelPortObject.PORT_TYPE, BufferedDataTable.TYPE, BufferedDataTable.TYPE}, //
            MODEL_TYPE, //
            JOB_ID, //
            new LinearLearnerSettings(LinearLearnerMode.LOGISTIC_REGRESSION));
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkMLModelPortObjectSpec modelPortSpec = (SparkMLModelPortObjectSpec)super.configureInternal(inSpecs)[0];
        return new PortObjectSpec[]{ //
            modelPortSpec, //
            null, // not available in configure
            null};  // not available in configure
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkMLModelPortObject modelPortObject = learnModel(inData, exec);
        final MLModel model = modelPortObject.getModel();
        final MLLogisticRegressionLearnerMetaData metaData = model.getModelMetaData(MLLogisticRegressionLearnerMetaData.class).orElseThrow();
        return new PortObject[]{ //
            modelPortObject, //
            createCoefficientsAndInterceptTable(exec, metaData), //
            createAccuracyStatisticsTable(exec, metaData)};

    }

    @Override
    protected MLLogisticRegressionLearnerJobInput createJobInput(final PortObject[] inData, final String newNamedModelId,
        final LinearLearnerSettings settings) throws InvalidSettingsException {

        final SparkDataPortObject data = (SparkDataPortObject)inData[0];
        final MLlibSettings mlSettings = settings.getSettings(data);

        return new MLLogisticRegressionLearnerJobInput( //
            data.getTableName(), //
            newNamedModelId, //
            mlSettings.getClassColIdx(), //
            mlSettings.getFeatueColIdxs(), //
            settings.getMaxIter(), //
            settings.useStandardization(), //
            settings.fitIntercept(), //
            settings.getRegularizer().name(), //
            settings.getRegParam(), //
            settings.getElasticNetParam(), //
            settings.getFamily().toSparkName(), //
            settings.getConvergenceTolerance(), //
            settings.getHandleInvalid().toSparkName());
    }

    private static DataTableSpec createCoefficientsAndInterceptTableSpec(final MLLogisticRegressionLearnerMetaData metaData) {
        final DataTableSpecCreator specCreator = new DataTableSpecCreator();

        if (metaData.isMultinominal()) {
            specCreator.addColumns(new DataColumnSpecCreator("Logit", StringCell.TYPE).createSpec());
        }

        specCreator.addColumns(new DataColumnSpecCreator("Variable", StringCell.TYPE).createSpec());
        specCreator.addColumns(new DataColumnSpecCreator("Coeff.", DoubleCell.TYPE).createSpec());

        return specCreator.createSpec();
    }

    private static BufferedDataTable createCoefficientsAndInterceptTable(final ExecutionContext exec,
        final MLLogisticRegressionLearnerMetaData metaData) {

        final List<String> targetLabels = metaData.coeffTagetLabels();
        final List<String> variableLabels = metaData.coeffVariableLabels();
        final List<Double> coefficients = metaData.coefficients();

        final BufferedDataContainer dataContainer = exec.createDataContainer(createCoefficientsAndInterceptTableSpec(metaData));
        for (int i = 0; i < coefficients.size(); i++) {
            final ArrayList<DataCell> rowCells = new ArrayList<>();
            if (metaData.isMultinominal()) {
                rowCells.add(new StringCell(targetLabels.get(i)));
            }
            rowCells.add(new StringCell(variableLabels.get(i)));
            rowCells.add(new DoubleCell(coefficients.get(i)));
            dataContainer.addRowToTable(new DefaultRow(RowKey.createRowKey(i + 1L), rowCells));
        }
        dataContainer.close();

        return dataContainer.getTable();
    }

    private static DataTableSpec createAccuracyStatisticsTableSpec(final MLLogisticRegressionLearnerMetaData metaData) {
        final DataTableSpecCreator specCreator = new DataTableSpecCreator();

        if (metaData.isMultinominal()) {
            specCreator.addColumns(new DataColumnSpecCreator("Target", StringCell.TYPE).createSpec());
        }

        return specCreator //
            .addColumns(new DataColumnSpecCreator("False Positive Rate", DoubleCell.TYPE).createSpec()) //
            .addColumns(new DataColumnSpecCreator("True Positive Rate", DoubleCell.TYPE).createSpec()) //
            .addColumns(new DataColumnSpecCreator("Recall", DoubleCell.TYPE).createSpec()) //
            .addColumns(new DataColumnSpecCreator("Precision", DoubleCell.TYPE).createSpec()) //
            .addColumns(new DataColumnSpecCreator("F-measure", DoubleCell.TYPE).createSpec()) //
            .addColumns(new DataColumnSpecCreator("Accuracy", DoubleCell.TYPE).createSpec()) //
            .createSpec();
    }

    private static BufferedDataTable createAccuracyStatisticsTable(final ExecutionContext exec,
        final MLLogisticRegressionLearnerMetaData metaData) {

        final List<String> targets = metaData.getNominalTargetValueMappings();
        final BufferedDataContainer dataContainer = exec.createDataContainer(createAccuracyStatisticsTableSpec(metaData));

        final List<double[]> rows = metaData.getAccuracyStatRows();
        int i;
        for (i = 0; i < rows.size(); i++) {
            final ArrayList<DataCell> rowCells = new ArrayList<>();
            if (metaData.isMultinominal()) {
                if (i + 1 < rows.size()) {
                    rowCells.add(new StringCell(targets.get(i)));
                } else {
                    rowCells.add(new StringCell("Weighted"));
                }
            }
            for (final double val : rows.get(i)) {
                rowCells.add(new DoubleCell(val));
            }
            if (metaData.isMultinominal()) {
                rowCells.add(new MissingCell(null)); // accuracy column
            } else {
                rowCells.add(new DoubleCell(metaData.getAccuracy()));
            }
            dataContainer.addRowToTable(new DefaultRow(RowKey.createRowKey(i + 1L), rowCells));
        }

        // overall accuracy row
        if (metaData.isMultinominal()) {
            dataContainer.addRowToTable(new DefaultRow( //
                RowKey.createRowKey(i + 1L), //
                new StringCell("Overall"), //
                new MissingCell(null), // false positive rate
                new MissingCell(null), // true positive rate
                new MissingCell(null), // recall
                new MissingCell(null), // percission
                new MissingCell(null), // f-measure
                new DoubleCell(metaData.getAccuracy()))); //
        }

        dataContainer.close();

        return dataContainer.getTable();
    }

}
