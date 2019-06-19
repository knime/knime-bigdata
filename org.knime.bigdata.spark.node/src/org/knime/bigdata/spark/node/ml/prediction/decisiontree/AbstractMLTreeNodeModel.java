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
 *   Created on May 26, 2016 by oole
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.bigdata.spark.core.node.SparkMLModelLearnerNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.model.ml.MLModelType;
import org.knime.bigdata.spark.core.port.model.ml.SparkMLModelPortObject;
import org.knime.bigdata.spark.core.port.model.ml.SparkMLModelPortObjectSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataTableSpecCreator;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Abstract superclass of all tree-based Spark ML model learner nodes.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @param <I> The {@link JobInput}
 * @param <T> The {@link MLlibNodeSettings}
 */
public abstract class AbstractMLTreeNodeModel<I extends MLDecisionTreeLearnerJobInput, T extends DecisionTreeSettings>
    extends SparkMLModelLearnerNodeModel<I, T> {

    /**
     * @param modelType
     * @param jobId
     * @param settings
     */
    protected AbstractMLTreeNodeModel(final MLModelType modelType, final String jobId,
        final T settings) {
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{BufferedDataTable.TYPE, SparkMLModelPortObject.PORT_TYPE},
            modelType,
            jobId,
            settings);
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkMLModelPortObjectSpec modelPortObjectSpec =
            (SparkMLModelPortObjectSpec)super.configureInternal(inSpecs)[0];
        return new PortObjectSpec[]{createFeatureImportanceTableOutputSpec(), modelPortObjectSpec};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkMLModelPortObject modelPortObject = learnModel(inData, exec);

        final BufferedDataTable featureImportancesPortObject = createFeatureImportanceTable(exec, modelPortObject);

        return new PortObject[] {featureImportancesPortObject, modelPortObject};
    }

    private static BufferedDataTable createFeatureImportanceTable(final ExecutionContext exec,
        final SparkMLModelPortObject modelPortObject) {

        final DataRow[] featureImportanceRows = createFeatureImportanceRows(modelPortObject);

        Arrays.sort(featureImportanceRows, new Comparator<DataRow>() {
            @Override
            public int compare(final DataRow left, final DataRow right) {
                return Double.compare(((DoubleCell)right.getCell(0)).getDoubleValue(),
                    ((DoubleCell)left.getCell(0)).getDoubleValue());
            }
        });

        final BufferedDataContainer featureImportanceContainer = exec.createDataContainer(createFeatureImportanceTableOutputSpec());
        for(DataRow row : featureImportanceRows) {
            featureImportanceContainer.addRowToTable(row);
        }
        featureImportanceContainer.close();

        return featureImportanceContainer.getTable();
    }

    private static DataRow[] createFeatureImportanceRows(final SparkMLModelPortObject modelPortObject) {

        final double[] featureImportances = modelPortObject.getModel()
            .getModelMetaData(GenericMLDecisionTreeMetaData.class).get().getFeatureImportances();

        final DataRow[] featureImportanceRows = new DataRow[featureImportances.length];

        int currFeature = 0;
        for(String learningColumn : modelPortObject.getModel().getLearningColumnNames()) {
            featureImportanceRows[currFeature] = new DefaultRow(learningColumn, new DoubleCell(featureImportances[currFeature]));
            currFeature++;
        }
        return featureImportanceRows;
    }

    private static DataTableSpec createFeatureImportanceTableOutputSpec() {
        final DataTableSpecCreator creator = new DataTableSpecCreator();
        creator.addColumns(new DataColumnSpecCreator("Feature Importance", DoubleCell.TYPE).createSpec());
        return creator.createSpec();
    }

    /**
     * @return the seed to use during learning.
     */
    protected int getSeed() {
        if (getSettings().useStaticSeed()) {
            return getSettings().getSeed();
        } else {
            return new Random().nextInt();
        }
    }
}