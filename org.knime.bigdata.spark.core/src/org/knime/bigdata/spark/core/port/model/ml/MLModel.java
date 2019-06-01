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
import java.io.Serializable;
import java.util.Optional;

import javax.swing.JComponent;

import org.knime.bigdata.spark.core.exception.MissingSparkModelHelperException;
import org.knime.bigdata.spark.core.job.util.ColumnBasedValueMapping;
import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.port.model.ModelHelperRegistry;
import org.knime.bigdata.spark.core.port.model.ModelInterpreter;
import org.knime.bigdata.spark.core.port.model.SparkModel;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.data.DataTableSpec;

/**
 * An {@link MLModel} holds a reference to a file that contains a saved Spark PipelineModel, which consists of a
 * sequence of transformation steps as well as an actual trained model. Moreover, it has a "named model ID", under which
 * it can be referenced on the Spark side.
 *
 * <p>
 * Additionally, an {@link MLModel} provides optional meta data about the model. First, there is a {@link Serializable}
 * meta data, which is commonly used to store a {@link ColumnBasedValueMapping}. However, beware that due to the use of
 * Java serialization, future changes to </p<>
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
     * Optional meta info about the model, that might be needed in the ModelInterpreter such as nominal value mapping
     * information.
     */
    private final Optional<MLMetaData> m_modelMetaData;

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
     * @param modelMetaData meta info about the model, that might be needed in the ModelInterpreter such as nominal
     *            value mapping information. May be null.
     * @throws MissingSparkModelHelperException
     */
    public MLModel(final SparkVersion sparkVersion, final String modelName, final File zippedPipelineModel,
        final String namedModelId, final MLlibSettings settings, final MLMetaData modelMetaData)
        throws MissingSparkModelHelperException {

        this(sparkVersion, modelName, zippedPipelineModel, namedModelId, settings.getLearningTableSpec(),
            settings.getClassColName(), modelMetaData);
    }

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param zippedPipelineModel File that contains the zipped PipelineModel.
     * @param namedModelId Key/ID of the named model on the Spark side. May be null.
     * @param spec the DataTableSpec of the table used to learn the model including the class column name
     * @param targetColName the name of the class column if appropriate otherwise <code>null</code>
     * @param modelMetaData meta info about the model, that might be needed in the ModelInterpreter such as mapping
     *            information. May be null.
     * @throws MissingSparkModelHelperException
     */
    public MLModel(final SparkVersion sparkVersion, final String modelName, final File zippedPipelineModel,
        final String namedModelId, final DataTableSpec spec, final String targetColName, final MLMetaData modelMetaData)
        throws MissingSparkModelHelperException {

        super(sparkVersion, modelName, spec, targetColName, null);
        m_zippedPipelineModel = zippedPipelineModel;
        m_namedModelId = namedModelId;
        m_modelMetaData = Optional.ofNullable(modelMetaData);
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
     * @param clazz Class of desired return type.
     * @return optional meta info about the model, that might be needed in the ModelInterpreter such as mapping
     *         information.
     */
    @SuppressWarnings("unchecked")
    public <T extends MLMetaData> Optional<T> getModelMetaData(final Class<T> clazz) {
        if (m_modelMetaData.isPresent()) {
            if (clazz == MLMetaData.class) {
                return (Optional<T>)m_modelMetaData;
            } else {
                try {
                    T instance = clazz.newInstance();
                    instance.setInternalMap(m_modelMetaData.get().getInternalMap());
                    return Optional.of(instance);
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        } else {
            return Optional.<T> empty();
        }
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

}
