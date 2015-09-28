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
 *   Created on Feb 12, 2015 by knime
 */
package com.knime.bigdata.spark.node.mllib.collaborativefiltering;

import java.util.Arrays;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelDouble;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.jobserver.server.CollaborativeFilteringModel;
import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.node.mllib.MLlibSettings;
import com.knime.bigdata.spark.node.mllib.prediction.predictor.MLlibPredictorNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.port.model.SparkModel;
import com.knime.bigdata.spark.port.model.SparkModelPortObject;
import com.knime.bigdata.spark.port.model.SparkModelPortObjectSpec;
import com.knime.bigdata.spark.util.SparkIDs;

/**
 *
 * @author knime
 */
public class MLlibCollaborativeFilteringNodeModel extends SparkNodeModel {

    private final SettingsModelString m_userCol = createUserColModel();

    private final SettingsModelString m_productCol = createProductColModel();

    private final SettingsModelString m_ratingCol = createRatingColModel();

    private final SettingsModelDouble m_lambda = createLambdaModel();

    /**
     *
     */
    public MLlibCollaborativeFilteringNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, SparkModelPortObject.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelDouble createLambdaModel() {
        return new SettingsModelDouble("lambda", 0.01);
    }

    /**
     * @return the user column model
     */
    static SettingsModelString createUserColModel() {
        return new SettingsModelString("userColumn", null);
    }

    /**
     * @return the product column model
     */
    static SettingsModelString createProductColModel() {
        return new SettingsModelString("productColumn", null);
    }

    /**
     * @return the rating column model
     */
    static SettingsModelString createRatingColModel() {
        return new SettingsModelString("ratingColumn", null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        final DataTableSpec tableSpec = spec.getTableSpec();
        //check that all columns are present in the input data
        createMLlibSettings(tableSpec);
        final DataTableSpec resultSpec = createResultTableSpec(tableSpec);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(spec.getContext(), resultSpec),
            new SparkModelPortObjectSpec(MatrixFactorizationModelInterpreter.getInstance().getModelName())};
    }

    /**
     * @param tableSpec
     * @return
     */
    private DataTableSpec createResultTableSpec(final DataTableSpec tableSpec) {
        return MLlibPredictorNodeModel.createSpec(tableSpec, "Rating");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[0];
        exec.setMessage("Collaborative filtering (SPARK)");
        exec.checkCanceled();
        final DataTableSpec tableSpec = data.getTableSpec();
        final MLlibSettings settings = createMLlibSettings(tableSpec);
        final int userIdx = settings.getFeatueColIdxs()[0];
        final int productIdx = settings.getFeatueColIdxs()[1];
        final int ratingIdx = settings.getClassColIdx();
        final double lambda = m_lambda.getDoubleValue();
        final CollaborativeFilteringTask task =
                new CollaborativeFilteringTask(data.getData(), userIdx, productIdx, ratingIdx, lambda);
        final String predictions = SparkIDs.createRDDID();
        final CollaborativeFilteringModel model = task.execute(exec, predictions);
        exec.setMessage("Collaborative filtering (SPARK) done.");
        final DataTableSpec resultSpec = createResultTableSpec(tableSpec);

        final SparkDataTable vMatrixRDD = new SparkDataTable(data.getContext(), predictions, resultSpec);

        final SparkModel<CollaborativeFilteringModel> sparkModel = new SparkModel<CollaborativeFilteringModel>(
                model, MatrixFactorizationModelInterpreter.getInstance(), settings);
        return new PortObject[]{new SparkDataPortObject(vMatrixRDD),
            new SparkModelPortObject<CollaborativeFilteringModel>(sparkModel)};
    }

    private MLlibSettings createMLlibSettings(final DataTableSpec tableSpec) throws InvalidSettingsException {
        final String[] featureColNames = new String[2];
        featureColNames [MatrixFactorizationModelInterpreter.SETTINGS_USER_COL_IDX] = m_userCol.getStringValue();
        featureColNames [MatrixFactorizationModelInterpreter.SETTINGS_PRODUCT_COL_IDX] = m_productCol.getStringValue();
        final Integer[] featureColIdxs = new Integer[2];
        featureColIdxs[MatrixFactorizationModelInterpreter.SETTINGS_USER_COL_IDX] =
                getColumnIndex(tableSpec, m_userCol, "User");
        featureColIdxs[MatrixFactorizationModelInterpreter.SETTINGS_PRODUCT_COL_IDX] =
                getColumnIndex(tableSpec, m_productCol, "Product");
        return new MLlibSettings(tableSpec, m_ratingCol.getStringValue(),
            getColumnIndex(tableSpec, m_ratingCol, "Ratings"), null, Arrays.asList(featureColNames), featureColIdxs,
            null);
    }

    private int getColumnIndex(final DataTableSpec spec, final SettingsModelString userCol, final String colType)
            throws InvalidSettingsException {
        final String colName = userCol.getStringValue();
        if (colName == null) {
            throw new InvalidSettingsException(colType + " column not selected");
        }
        final int colIdx = spec.findColumnIndex(colName);
        if (colIdx < 0) {
            throw new InvalidSettingsException(colType + " column with name: + " + colName + " not found in input data");
        }
        return colIdx;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_productCol.saveSettingsTo(settings);
        m_userCol.saveSettingsTo(settings);
        m_lambda.saveSettingsTo(settings);
        m_ratingCol.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_productCol.validateSettings(settings);
        m_userCol.validateSettings(settings);
        m_lambda.validateSettings(settings);
        m_ratingCol.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_productCol.loadSettingsFrom(settings);
        m_userCol.loadSettingsFrom(settings);
        m_lambda.loadSettingsFrom(settings);
        m_ratingCol.loadSettingsFrom(settings);
    }
}
