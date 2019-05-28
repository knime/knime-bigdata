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
 *   Created on Feb 12, 2015 by knime
 */
package org.knime.bigdata.spark.node.mllib.collaborativefiltering;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.job.JobRunFactory;
import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.port.model.MLlibModel;
import org.knime.bigdata.spark.core.port.model.SparkModelPortObject;
import org.knime.bigdata.spark.core.port.model.SparkModelPortObjectSpec;
import org.knime.bigdata.spark.node.mllib.prediction.predictor.MLlibPredictorNodeModel;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelDouble;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelLong;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 *
 * @author knime
 */
public class MLlibCollaborativeFilteringNodeModel extends SparkNodeModel {

    /**The unique model name.*/
    public static final String MODEL_NAME = "Matrix Factorization Model";

    /** default random seed for cluster initialization (use default ml based model seed) */
    public static final long DEFAULT_SEED = "org.apache.spark.ml.param.shared.HasSeed".hashCode();

    private static final String OUTPUT_COL_NAME = "Rating";

    private static final String SAME_INPUT_COLS_ERROR = "Different input columns for user, product and rating required.";

    private final SettingsModelString m_userCol = createUserColModel();
    private final SettingsModelString m_productCol = createProductColModel();
    private final SettingsModelString m_ratingCol = createRatingColModel();
    private final SettingsModelDouble m_lambda = createLambdaModel();
    private final SettingsModelDouble m_alpha = createAlphaModel();
    private final SettingsModelInteger m_rank = createRankModel();
    private final SettingsModelInteger m_iterations = createIterationsModel();
    private final SettingsModelInteger m_blocks = createNoOfBlocksModel();
    private final SettingsModelBoolean m_implicitPrefs = createImplicitPrefsModel();
    private final SettingsModelLong m_seed = createSeedModel();

    /**The unique Spark job id.*/
    public static final String JOB_ID = "MLlibCollaborativeFilteringJob";

    /**
     *
     */
    public MLlibCollaborativeFilteringNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, SparkModelPortObject.TYPE});
    }

    static SettingsModelDouble createAlphaModel() {
        return new SettingsModelDouble("alpha", 1);
    }

    /**
    * @return
    */
    static SettingsModelInteger createRankModel() {
        return new SettingsModelInteger("rank", 10);
    }

    /**
    * @return
    */
    static SettingsModelInteger createIterationsModel() {
        return new SettingsModelInteger("iterations", 10);
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
     * @return the number of blocks model
     */
    static SettingsModelInteger createNoOfBlocksModel() {
        return new SettingsModelIntegerBounded("numberOfBlocks", -1, -1, Integer.MAX_VALUE);
    }

    /**
     * @return the implicit preferences model
     */
    static SettingsModelBoolean createImplicitPrefsModel() {
        return new SettingsModelBoolean("implicitPrefs", false);
    }

    /** @return random seed model */
    static SettingsModelLong createSeedModel() {
        return new SettingsModelLong("seed", DEFAULT_SEED);
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
        final String resultCol = DataTableSpec.getUniqueColumnName(tableSpec, OUTPUT_COL_NAME);
        final DataTableSpec resultSpec = MLlibPredictorNodeModel.createSpec(tableSpec, resultCol);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(spec.getContextID(), resultSpec),
            new SparkModelPortObjectSpec(getSparkVersion(spec), MODEL_NAME)};
    }

    /**
     * Creates output spec with appended double column with given (unique) name.
     *
     * @param inputSpec the input data spec
     * @param resultColName the name of the result column (has to be unique)
     * @return the result spec with appended double column
     */
    public static DataTableSpec createSpec(final DataTableSpec inputSpec, final String resultColName) {
        final DataColumnSpecCreator creator = new DataColumnSpecCreator(resultColName, DoubleCell.TYPE);
        return new DataTableSpec(inputSpec, new DataTableSpec(creator.createSpec()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inObjects[0];
        final JobRunFactory<CollaborativeFilteringJobInput, CollaborativeFilteringJobOutput> runFactory =
                SparkContextUtil.getJobRunFactory(data.getContextID(), JOB_ID);
        exec.setMessage("Preparing Collaborative filtering job");
        exec.checkCanceled();
        final DataTableSpec tableSpec = data.getTableSpec();
        final MLlibSettings settings = createMLlibSettings(tableSpec);
        final int userIdx = settings.getFeatueColIdxs()[CollaborativeFilteringJobInput.MLLIB_SETTINGS_USER_COL_IDX];
        final int productIdx =
                settings.getFeatueColIdxs()[CollaborativeFilteringJobInput.MLLIB_SETTINGS_PRODUCT_COL_IDX];
        final int ratingIdx = settings.getClassColIdx();
        final double lambda = m_lambda.getDoubleValue();
        final double alpha = m_alpha.getDoubleValue();
        final int iterations = m_iterations.getIntValue();
        final int rank = m_rank.getIntValue();
        final int noOfBlocks = m_blocks.getIntValue();
        final boolean implicitPrefs = m_implicitPrefs.getBooleanValue();
        final long seed = m_seed.getLongValue();
        //vMatrix is the prediction result
        final String resultCol = DataTableSpec.getUniqueColumnName(tableSpec, OUTPUT_COL_NAME);
        final DataTableSpec vMatrixSpec = createSpec(tableSpec, resultCol);
        final SparkDataTable vMatrixRDD = new SparkDataTable(data.getContextID(), vMatrixSpec);
        exec.checkCanceled();
        final CollaborativeFilteringJobInput jobInput =
            new CollaborativeFilteringJobInput(data.getTableName(), vMatrixRDD.getID(), userIdx, productIdx, ratingIdx,
                resultCol, lambda, alpha, iterations, rank, implicitPrefs, noOfBlocks, seed);
        exec.setMessage("Running Collaborative filtering job");
        final CollaborativeFilteringJobOutput output = runFactory.createRun(jobInput).run(data.getContextID());
        final MLlibModel sparkModel = new MLlibModel(
            SparkContextManager.getOrCreateSparkContext(data.getContextID()).getSparkVersion(), MODEL_NAME,
            output.getModel(), settings);
        //add the model RDDs to the list of RDDs to delete on reset
        addAdditionalSparkDataObjectsToDelete(data.getContextID(), output.getUserFeaturesObjectName(),
            output.getProductFeaturesObjectName());
        return new PortObject[]{new SparkDataPortObject(vMatrixRDD), new SparkModelPortObject(sparkModel)};
    }

    private MLlibSettings createMLlibSettings(final DataTableSpec tableSpec) throws InvalidSettingsException {
        Set<String> colNames = new HashSet<>(3);
        final String[] featureColNames = new String[2];
        featureColNames [CollaborativeFilteringJobInput.MLLIB_SETTINGS_USER_COL_IDX] = m_userCol.getStringValue();
        colNames.add(m_userCol.getStringValue());
        featureColNames [CollaborativeFilteringJobInput.MLLIB_SETTINGS_PRODUCT_COL_IDX] = m_productCol.getStringValue();
        if (!colNames.add(m_productCol.getStringValue())) {
            throw new InvalidSettingsException(SAME_INPUT_COLS_ERROR);
        }
        final Integer[] featureColIdxs = new Integer[2];
        featureColIdxs[CollaborativeFilteringJobInput.MLLIB_SETTINGS_USER_COL_IDX] =
                getColumnIndex(tableSpec, m_userCol, "User");
        featureColIdxs[CollaborativeFilteringJobInput.MLLIB_SETTINGS_PRODUCT_COL_IDX] =
                getColumnIndex(tableSpec, m_productCol, "Product");
        if (!colNames.add(m_ratingCol.getStringValue())) {
            throw new InvalidSettingsException(SAME_INPUT_COLS_ERROR);
        }
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
            throw new InvalidSettingsException(colType + " column with name: " + colName + " not found in input data");
        }
        return colIdx;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_productCol.saveSettingsTo(settings);
        m_userCol.saveSettingsTo(settings);
        m_lambda.saveSettingsTo(settings);
        m_ratingCol.saveSettingsTo(settings);
        m_alpha.saveSettingsTo(settings);
        m_iterations.saveSettingsTo(settings);
        m_rank.saveSettingsTo(settings);
        m_blocks.saveSettingsTo(settings);
        m_implicitPrefs.saveSettingsTo(settings);
        m_seed.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_productCol.validateSettings(settings);
        m_userCol.validateSettings(settings);
        m_lambda.validateSettings(settings);
        m_ratingCol.validateSettings(settings);
        m_alpha.validateSettings(settings);
        m_iterations.validateSettings(settings);
        m_rank.validateSettings(settings);
        m_blocks.validateSettings(settings);
        m_implicitPrefs.validateSettings(settings);

        try {
            m_seed.validateSettings(settings);
        } catch (InvalidSettingsException e) {
            // option setting
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_productCol.loadSettingsFrom(settings);
        m_userCol.loadSettingsFrom(settings);
        m_lambda.loadSettingsFrom(settings);
        m_ratingCol.loadSettingsFrom(settings);
        m_alpha.loadSettingsFrom(settings);
        m_iterations.loadSettingsFrom(settings);
        m_rank.loadSettingsFrom(settings);
        m_blocks.loadSettingsFrom(settings);
        m_implicitPrefs.loadSettingsFrom(settings);

        try {
            m_seed.loadSettingsFrom(settings);
        } catch (InvalidSettingsException e) {
            // option setting
        }
    }
}
