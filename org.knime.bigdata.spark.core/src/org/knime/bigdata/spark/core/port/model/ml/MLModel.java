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
 *   Created on May 25, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.port.model.ml;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;

import javax.swing.JComponent;

import org.knime.bigdata.spark.core.exception.MissingSparkModelHelperException;
import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.port.model.ModelHelperRegistry;
import org.knime.bigdata.spark.core.port.model.ModelInterpreter;
import org.knime.bigdata.spark.core.port.model.SparkModel;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.data.DataTableSpec;
import org.knime.core.util.FileUtil;

/**
 * An instance of this class holds a reference to a file that contains a saved Spark PipelineModel, that holds a
 * sequence of transformation steps as well as an actual trained model. Moreover, it has a "named model ID", under which
 * it can be referenced on the Spark side.
 *
 * <p>
 * Note: The "ML" in MLModel refers to the Spark ML packages, that have superseded the MLLib packages.
 * </p>
 *
 *
 * @author Bjoern Lohrmann
 * @since 2.5
 */
public class MLModel extends SparkModel {

    /**
     * A file that contains the zipped Pipeline model.
     */
    private File m_zippedPipelineModel;

    /**
     * Key/ID of the named model in the Spark context where the model was learned. May be null.
     */
    private final String m_namedModelId;

    /**
     * The model interpreter for the model.
     */
    private final ModelInterpreter<MLModel> m_interpreter;

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param zippedPipelineModel File that contains the zipped PipelineModel.
     * @param namedModelId Key/ID of the named model on the Spark side. May be null.
     * @param settings {@link MLlibSettings} used when learning the model
     * @throws MissingSparkModelHelperException
     */
    public MLModel(final SparkVersion sparkVersion, final String modelName, final File zippedPipelineModel,
        final String namedModelId, final MLlibSettings settings) throws MissingSparkModelHelperException {
        this(sparkVersion, modelName, zippedPipelineModel, namedModelId, settings, null);
    }

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param zippedPipelineModel File that contains the zipped PipelineModel.
     * @param namedModelId Key/ID of the named model on the Spark side. May be null.
     * @param settings {@link MLlibSettings} used when learning the model
     * @param metaData any additional meta information that might be needed in the ModelInterpreter such as mapping
     *            information
     * @throws MissingSparkModelHelperException
     */
    public MLModel(final SparkVersion sparkVersion, final String modelName, final File zippedPipelineModel,
        final String namedModelId, final MLlibSettings settings, final Serializable metaData)
        throws MissingSparkModelHelperException {
        this(sparkVersion, modelName, zippedPipelineModel, namedModelId, settings.getLearningTableSpec(),
            settings.getClassColName(), settings.getFatureColNames(), metaData);
    }

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param zippedPipelineModel File that contains the zipped PipelineModel.
     * @param namedModelId Key/ID of the named model on the Spark side. May be null.
     * @param origSpec the {@link DataTableSpec} of the original input table
     * @param targetColName the name of the class column if appropriate otherwise <code>null</code>
     * @param featureColNames the names of the feature columns in the order they where used when learning the model
     * @throws MissingSparkModelHelperException
     */
    public MLModel(final SparkVersion sparkVersion, final String modelName, final File zippedPipelineModel,
        final String namedModelId, final DataTableSpec origSpec, final String targetColName,
        final List<String> featureColNames) throws MissingSparkModelHelperException {
        this(sparkVersion, modelName, zippedPipelineModel, namedModelId, origSpec, targetColName, featureColNames,
            null);
    }

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param zippedPipelineModel File that contains the zipped PipelineModel.
     * @param namedModelId Key/ID of the named model on the Spark side. May be null.
     * @param metaData any additional meta information that might be needed in the ModelInterpreter such as mapping
     *            information
     * @param origSpec the {@link DataTableSpec} of the original input table
     * @param targetColName the name of the class column if appropriate otherwise <code>null</code>
     * @param featureColNames the names of the feature columns in the order they where used when learning the model
     * @throws MissingSparkModelHelperException
     */
    public MLModel(final SparkVersion sparkVersion, final String modelName, final File zippedPipelineModel,
        final String namedModelId, final DataTableSpec origSpec, final String targetColName,
        final List<String> featureColNames, final Serializable metaData) throws MissingSparkModelHelperException {
        this(sparkVersion, modelName, zippedPipelineModel, namedModelId,
            MLlibSettings.createLearningSpec(origSpec, targetColName, featureColNames), targetColName, metaData);
    }

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param zippedPipelineModel File that contains the zipped PipelineModel.
     * @param namedModelId Key/ID of the named model on the Spark side. May be null.
     * @param origSpec the {@link DataTableSpec} of the original input table
     * @param targetColName the name of the class column if appropriate otherwise <code>null</code>
     * @param featureColNames the names of the feature columns in the order they where used when learning the model
     * @throws MissingSparkModelHelperException
     */
    public MLModel(final SparkVersion sparkVersion, final String modelName, final File zippedPipelineModel,
        final String namedModelId, final DataTableSpec origSpec, final String targetColName,
        final String... featureColNames) throws MissingSparkModelHelperException {
        this(sparkVersion, modelName, zippedPipelineModel, namedModelId,
            MLlibSettings.createLearningSpec(origSpec, targetColName, featureColNames), targetColName);
    }

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param zippedPipelineModel File that contains the zipped PipelineModel.
     * @param namedModelId Key/ID of the named model on the Spark side. May be null.
     * @param spec the DataTableSpec of the table used to learn the model including the class column name
     * @param targetColName the name of the class column if appropriate otherwise <code>null</code>
     * @throws MissingSparkModelHelperException
     */
    public MLModel(final SparkVersion sparkVersion, final String modelName, final File zippedPipelineModel,
        final String namedModelId, final DataTableSpec spec, final String targetColName)
        throws MissingSparkModelHelperException {
        this(sparkVersion, modelName, zippedPipelineModel, namedModelId, spec, targetColName, (Serializable)null);
    }

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param zippedPipelineModel File that contains the zipped PipelineModel.
     * @param namedModelId Key/ID of the named model on the Spark side. May be null.
     * @param metaData any additional meta information that might be needed in the ModelInterpreter such as mapping
     *            information. May be null.
     * @param spec the DataTableSpec of the table used to learn the model including the class column name
     * @param targetColName the name of the class column if appropriate otherwise <code>null</code>
     * @throws MissingSparkModelHelperException
     */
    public MLModel(final SparkVersion sparkVersion, final String modelName, final File zippedPipelineModel,
        final String namedModelId, final DataTableSpec spec, final String targetColName, final Serializable metaData)
        throws MissingSparkModelHelperException {

        super(sparkVersion, modelName, spec, targetColName, metaData);
        m_zippedPipelineModel = zippedPipelineModel;
        m_namedModelId = namedModelId;
        m_interpreter = ModelHelperRegistry.getMLModelHelper(modelName, sparkVersion).getModelInterpreter();
    }

    /**
     * @return a File that holds the zipped Spark PipelineModel.
     */
    public File getZippedPipelineModel() {
        return m_zippedPipelineModel;
    }

    void setZippedPipelineModel(final File zippedPipelineModel) {
        m_zippedPipelineModel = zippedPipelineModel;
    }

    /**
     * @return key/ID of the named model on the Spark side.
     */
    public String getNamedModelId() {
        return m_namedModelId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ModelInterpreter<MLModel> getInterpreter() {
        return m_interpreter;
    }

    /**
     * @return the summary of this model to use in the port tooltip
     */
    @Override
    public String getSummary() {
        return m_interpreter.getSummary(this);
    }

    /**
     * @return the model views
     */
    @Override
    public JComponent[] getViews() {
        return getInterpreter().getViews(this);
    }

    /**
     * Utility method to unpack the zipped Pipeline model in the given {@link MLModel} to a temp directory. It is up to
     * the caller to delete the temp directory and its contents.
     *
     * @return a temp directory that contains the unzipped, saved Pipeline model.
     * @throws IOException When something went wrong while unzipping the model.
     */
    public Path unpackZippedPipelineModel() throws IOException {
        final File tempDir = FileUtil.createTempDir("SparkNamedModel", null, true);
        FileUtil.unzip(getZippedPipelineModel(), tempDir);
        return tempDir.toPath();
    }
}
