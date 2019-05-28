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
package org.knime.bigdata.spark.core.port.model;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.zip.ZipEntry;

import javax.swing.JComponent;

import org.knime.bigdata.spark.core.exception.MissingSparkModelHelperException;
import org.knime.bigdata.spark.core.job.util.MLlibSettings;
import org.knime.bigdata.spark.core.model.LegacyModelHelper;
import org.knime.bigdata.spark.core.model.MLlibModelHelper;
import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;

/**
 * Spark model that encapsulates a learned Spark MLlib model.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibModel extends SparkModel {

    private static final String ZIP_KEY_MODEL_META_DATA = "modelMetaData";

    private static final String ZIP_KEY_NATIVE_MODEL = "nativeModel";

    private static final String ZIP_KEY_TABLE_SPEC = "tableSpec";

    private static final String KEY_CLASS_COLUMN_NAME = "classColumnName";

    private static final String KEY_MODEL_NAME = "modelName";

    private static final String KEY_SPARK_VERSION = "sparkVersion";

    private static final String LEGACY_MODEL_ENTRY = "Model";

    private static final String ZIP_KEY_MODEL_1_6_HEADER = "Modelv1_6";

    /**
     * The model object itself.
     */
    private final Serializable m_model;

    /**
     * The model interpreter for the model.
     */
    private final ModelInterpreter<MLlibModel> m_interpreter;

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param model the model itself
     * @param settings {@link MLlibSettings} used when learning the model
     * @throws MissingSparkModelHelperException
     */
    public MLlibModel(final SparkVersion sparkVersion, final String modelName, final Serializable model,
        final MLlibSettings settings) throws MissingSparkModelHelperException {
        this(sparkVersion, modelName, model, settings, null);
    }

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param model the model itself
     * @param settings {@link MLlibSettings} used when learning the model
     * @param metaData any additional meta information that might be needed in the ModelInterpreter such as mapping
     *            information
     * @throws MissingSparkModelHelperException
     */
    public MLlibModel(final SparkVersion sparkVersion, final String modelName, final Serializable model,
        final MLlibSettings settings, final Serializable metaData) throws MissingSparkModelHelperException {
        this(sparkVersion, modelName, model, settings.getLearningTableSpec(), settings.getClassColName(),
            settings.getFatureColNames(), metaData);
    }

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param model the model itself
     * @param origSpec the {@link DataTableSpec} of the original input table
     * @param classColName the name of the class column if appropriate otherwise <code>null</code>
     * @param featureColNames the names of the feature columns in the order they where used when learning the model
     * @throws MissingSparkModelHelperException
     */
    public MLlibModel(final SparkVersion sparkVersion, final String modelName, final Serializable model,
        final DataTableSpec origSpec, final String classColName, final List<String> featureColNames)
            throws MissingSparkModelHelperException {
        this(sparkVersion, modelName, model, origSpec, classColName, featureColNames, null);
    }

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param model the model itself
     * @param metaData any additional meta information that might be needed in the ModelInterpreter such as mapping
     *            information
     * @param origSpec the {@link DataTableSpec} of the original input table
     * @param classColName the name of the class column if appropriate otherwise <code>null</code>
     * @param featureColNames the names of the feature columns in the order they where used when learning the model
     * @throws MissingSparkModelHelperException
     */
    public MLlibModel(final SparkVersion sparkVersion, final String modelName, final Serializable model,
        final DataTableSpec origSpec, final String classColName, final List<String> featureColNames,
        final Serializable metaData) throws MissingSparkModelHelperException {
        this(sparkVersion, modelName, model, MLlibSettings.createLearningSpec(origSpec, classColName, featureColNames),
            classColName, metaData);
    }

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param model the model itself
     * @param origSpec the {@link DataTableSpec} of the original input table
     * @param classColName the name of the class column if appropriate otherwise <code>null</code>
     * @param featureColNames the names of the feature columns in the order they where used when learning the model
     * @throws MissingSparkModelHelperException
     */
    public MLlibModel(final SparkVersion sparkVersion, final String modelName, final Serializable model,
        final DataTableSpec origSpec, final String classColName, final String... featureColNames)
            throws MissingSparkModelHelperException {
        this(sparkVersion, modelName, model, MLlibSettings.createLearningSpec(origSpec, classColName, featureColNames),
            classColName);
    }

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param model the model itself
     * @param spec the DataTableSpec of the table used to learn the model including the class column name
     * @param classColName the name of the class column if appropriate otherwise <code>null</code>
     * @throws MissingSparkModelHelperException
     */
    public MLlibModel(final SparkVersion sparkVersion, final String modelName, final Serializable model,
        final DataTableSpec spec, final String classColName) throws MissingSparkModelHelperException {
        this(sparkVersion, modelName, model, spec, classColName, (Serializable)null);
    }

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param model the model itself
     * @param metaData any additional meta information that might be needed in the ModelInterpreter such as mapping
     *            information
     * @param spec the DataTableSpec of the table used to learn the model including the class column name
     * @param classColName the name of the class column if appropriate otherwise <code>null</code>
     * @throws MissingSparkModelHelperException
     */
    @SuppressWarnings("unchecked")
    public MLlibModel(final SparkVersion sparkVersion, final String modelName, final Serializable model,
        final DataTableSpec spec, final String classColName, final Serializable metaData)
            throws MissingSparkModelHelperException {
        super(sparkVersion, modelName, spec, classColName, metaData);
        m_model = model;
        m_interpreter = ModelHelperRegistry.getModelHelper(modelName, sparkVersion).getModelInterpreter();
    }

    /**
     * @param exec
     * @param in
     * @return the {@link MLlibModel} read from the given stream
     * @throws IOException
     */
    public static MLlibModel load(final ExecutionMonitor exec, final PortObjectZipInputStream in) throws IOException {

        final ZipEntry headerOrLegacyModelEntry = in.getNextEntry();

        try {
            if (headerOrLegacyModelEntry.getName().equals(LEGACY_MODEL_ENTRY)) {
                return loadLegacySparkModel(in);
            } else if (headerOrLegacyModelEntry.getName().equals(ZIP_KEY_MODEL_1_6_HEADER)) {
                return load_v1_6_model(in);
            }
        } catch (InvalidSettingsException | ClassNotFoundException | MissingSparkModelHelperException e) {
            throw new IOException(e);
        }

        throw new IOException("Invalid zip entry");
    }

    @SuppressWarnings("resource")
    private static MLlibModel load_v1_6_model(final PortObjectZipInputStream in)
        throws IOException, InvalidSettingsException, MissingSparkModelHelperException {

        // read the header
        final NodeSettingsRO header = NodeSettings.loadFromXML(new NonClosableInputStream.Zip(in));
        final SparkVersion sparkVersion = SparkVersion.fromString(header.getString(KEY_SPARK_VERSION));
        final String modelName = header.getString(KEY_MODEL_NAME);
        final String classColumnName = header.getString(KEY_CLASS_COLUMN_NAME);

        // read the table spec
        in.getNextEntry();
        final DataTableSpec tableSpec =
            DataTableSpec.load(NodeSettings.loadFromXML(new NonClosableInputStream.Zip(in)));

        final MLlibModelHelper modelHelper = ModelHelperRegistry.getModelHelper(modelName, sparkVersion);

        // deserialize the model using a SparkModelHelper (which comes from a jobs plugin and hence has the classes
        // required for deserialization
        in.getNextEntry();
        final Serializable model = modelHelper.loadModel(new NonClosableInputStream.Zip(in));

        in.getNextEntry();
        final Serializable metaData = modelHelper.loadMetaData(new NonClosableInputStream.Zip(in));

        return new MLlibModel(sparkVersion, modelName, model, tableSpec, classColumnName, metaData);
    }

    private static MLlibModel loadLegacySparkModel(final PortObjectZipInputStream in)
        throws IOException, MissingSparkModelHelperException, ClassNotFoundException, InvalidSettingsException {

        final SparkVersion sparkVersion;
        if (KNIMEConfigContainer.getSparkVersion().equals(SparkVersion.V_1_2) || KNIMEConfigContainer.getSparkVersion().equals(SparkVersion.V_1_3)) {
            sparkVersion = KNIMEConfigContainer.getSparkVersion();
        } else {
            sparkVersion = SparkVersion.V_1_2;
        }

        final LegacyModelHelper legacySparkModelHelper =
            (LegacyModelHelper)ModelHelperRegistry.getModelHelper(LegacyModelHelper.LEGACY_MODEL_NAME, sparkVersion);

        try (final ObjectInputStream os = legacySparkModelHelper.getObjectInputStream(in)) {
            final String classColumnName = (String)os.readObject();
            final Serializable legacyModel = (Serializable)os.readObject();

            // this reads an old model interpreter and throws it away because we don't actually need it (however we need what
            // comes after it).
            os.readObject();

            final String modelName = legacySparkModelHelper.tryToGuessModelName(legacyModel);
            if (modelName == null) {
                throw new IOException("Failed to guess model name of model instance: " + legacyModel.getClass().getName());
            }

            final Serializable convertedModel = (Serializable) legacySparkModelHelper.convertLegacyToNewModel(legacyModel);

            final DataTableSpec tableSpec = DataTableSpec.load((NodeSettings)os.readObject());

            return new MLlibModel(sparkVersion, modelName, convertedModel, tableSpec, classColumnName, (Serializable)null);
        }
    }

    /**
     * Saves the model to the given stream
     *
     * @param exec
     * @param out
     * @throws IOException
     */
    @SuppressWarnings("resource")
    public void write(final ExecutionMonitor exec, final PortObjectZipOutputStream out) throws IOException {
        out.putNextEntry(new ZipEntry(ZIP_KEY_MODEL_1_6_HEADER));
        NodeSettings header = new NodeSettings(ZIP_KEY_MODEL_1_6_HEADER);
        header.addString(KEY_SPARK_VERSION, getSparkVersion().toString());
        header.addString(KEY_MODEL_NAME, getModelName());
        header.addString(KEY_CLASS_COLUMN_NAME, getTargetColumnName());
        header.saveToXML(new NonClosableOutputStream.Zip(out));

        out.putNextEntry(new ZipEntry(ZIP_KEY_TABLE_SPEC));
        final NodeSettings tableSpec = new NodeSettings(ZIP_KEY_TABLE_SPEC);
        getTableSpec().save(tableSpec);
        tableSpec.saveToXML(new NonClosableOutputStream.Zip(out));

        MLlibModelHelper modelHelper;
        try {
            modelHelper = ModelHelperRegistry.getModelHelper(getModelName(), getSparkVersion());
        } catch (MissingSparkModelHelperException e) {
            throw new IOException(e);
        }

        // serialize the model using a SparkModelHelper (which comes from a jobs plugin and hence has the classes
        // required for serialization
        out.putNextEntry(new ZipEntry(ZIP_KEY_NATIVE_MODEL));
        modelHelper.saveModel(new NonClosableOutputStream.Zip(out), m_model);

        // serialize the model meta data using a SparkModelHelper (which comes from a jobs plugin and hence has the classes
        // required for serialization
        out.putNextEntry(new ZipEntry(ZIP_KEY_MODEL_META_DATA));
        modelHelper.saveModelMetadata(new NonClosableOutputStream.Zip(out), getMetaData());
    }

    /**
     * @return the spec
     */
    @Override
    public SparkModelPortObjectSpec getSpec() {
        return new SparkModelPortObjectSpec(getSparkVersion(), getModelName());
    }

    /**
     * @return the model
     */
    public Serializable getModel() {
        return m_model;
    }

    /**
     * @return the summary of this model to use in the port tooltip
     */
    @Override
    public String getSummary() {
        return getInterpreter().getSummary(this);
    }

    /**
     * @return the model views
     */
    @Override
    public JComponent[] getViews() {
        return getInterpreter().getViews(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ModelInterpreter<MLlibModel> getInterpreter() {
        return m_interpreter;
    }
}
