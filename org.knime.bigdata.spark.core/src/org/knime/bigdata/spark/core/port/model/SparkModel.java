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
package com.knime.bigdata.spark.core.port.model;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;

import javax.swing.JComponent;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;

import com.knime.bigdata.spark.core.exception.MissingSparkModelHelperException;
import com.knime.bigdata.spark.core.job.util.MLlibSettings;
import com.knime.bigdata.spark.core.model.LegacyModelHelper;
import com.knime.bigdata.spark.core.model.ModelHelper;
import com.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Spark model that encapsulates a learned Spark MLlib model.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkModel {

    /**
     *
     */
    private static final String ZIP_KEY_MODEL_META_DATA = "modelMetaData";

    /**
     *
     */
    private static final String ZIP_KEY_NATIVE_MODEL = "nativeModel";

    /**
     *
     */
    private static final String ZIP_KEY_TABLE_SPEC = "tableSpec";

    /**
     *
     */
    private static final String SETTINGS_KEY_MODEL_HEADER = "modelHeader";

    /**
     *
     */
    private static final String KEY_CLASS_COLUMN_NAME = "classColumnName";

    /**
     *
     */
    private static final String KEY_MODEL_NAME = "modelName";

    /**
     *
     */
    private static final String KEY_SPARK_VERSION = "sparkVersion";

    private static final String LEGACY_MODEL_ENTRY = "Model";

    private static final String ZIP_KEY_MODEL_1_6_HEADER = "Modelv1_6";

    private final Serializable m_model;

    private final Serializable m_metaData;

    /** DataTableSpec of the table used to learn the model including the class column name. */
    private final DataTableSpec m_tableSpec;

    private final String m_classColumnName;

    private final ModelInterpreter m_interpreter;

    /**
     * The Spark version under which the model was created.
     *
     * @since 1.6.0
     */
    private final SparkVersion m_sparkVersion;

    /**
     * The name of the model e.g. KMeans
     *
     * @since 1.6.0
     */
    private final String m_modelName;

    /**
     * @param sparkVersion the Spark version the model was learned with
     * @param modelName the unique name of the model
     * @param model the model itself
     * @param settings {@link MLlibSettings} used when learning the model
     * @throws MissingSparkModelHelperException
     */
    public SparkModel(final SparkVersion sparkVersion, final String modelName, final Serializable model,
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
    public SparkModel(final SparkVersion sparkVersion, final String modelName, final Serializable model,
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
    public SparkModel(final SparkVersion sparkVersion, final String modelName, final Serializable model,
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
    public SparkModel(final SparkVersion sparkVersion, final String modelName, final Serializable model,
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
    public SparkModel(final SparkVersion sparkVersion, final String modelName, final Serializable model,
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
    public SparkModel(final SparkVersion sparkVersion, final String modelName, final Serializable model,
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
    public SparkModel(final SparkVersion sparkVersion, final String modelName, final Serializable model,
        final DataTableSpec spec, final String classColName, final Serializable metaData)
            throws MissingSparkModelHelperException {

        m_sparkVersion = sparkVersion;
        m_modelName = modelName;
        m_model = model;
        m_interpreter = ModelHelperRegistry.getModelHelper(m_modelName, m_sparkVersion).getModelInterpreter();
        m_tableSpec = spec;
        m_classColumnName = classColName;
        m_metaData = metaData;
    }

    /**
     * @param exec
     * @param in
     * @return the {@link SparkModel} read from the given stream
     * @throws IOException
     */
    public static SparkModel load(final ExecutionMonitor exec, final PortObjectZipInputStream in) throws IOException {

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

    private static SparkModel load_v1_6_model(final PortObjectZipInputStream in)
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

        final ModelHelper modelHelper = ModelHelperRegistry.getModelHelper(modelName, sparkVersion);

        // deserialize the model using a SparkModelHelper (which comes from a jobs plugin and hence has the classes
        // required for deserialization
        in.getNextEntry();
        final Serializable model = modelHelper.loadModel(new NonClosableInputStream.Zip(in));

        in.getNextEntry();
        final Serializable metaData = modelHelper.loadMetaData(new NonClosableInputStream.Zip(in));

        return new SparkModel(sparkVersion, modelName, model, tableSpec, classColumnName, metaData);
    }

    private static SparkModel loadLegacySparkModel(final PortObjectZipInputStream in)
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

            return new SparkModel(sparkVersion, modelName, convertedModel, tableSpec, classColumnName, (Serializable)null);
        }
    }

    /**
     * Saves the model to the given stream
     *
     * @param exec
     * @param out
     * @throws IOException
     */
    public void write(final ExecutionMonitor exec, final PortObjectZipOutputStream out) throws IOException {
        out.putNextEntry(new ZipEntry(ZIP_KEY_MODEL_1_6_HEADER));
        NodeSettings header = new NodeSettings(ZIP_KEY_MODEL_1_6_HEADER);
        header.addString(KEY_SPARK_VERSION, m_sparkVersion.toString());
        header.addString(KEY_MODEL_NAME, m_modelName);
        header.addString(KEY_CLASS_COLUMN_NAME, m_classColumnName);
        header.saveToXML(new NonClosableOutputStream.Zip(out));

        out.putNextEntry(new ZipEntry(ZIP_KEY_TABLE_SPEC));
        final NodeSettings tableSpec = new NodeSettings(ZIP_KEY_TABLE_SPEC);
        m_tableSpec.save(tableSpec);
        tableSpec.saveToXML(new NonClosableOutputStream.Zip(out));

        ModelHelper modelHelper;
        try {
            modelHelper = ModelHelperRegistry.getModelHelper(m_modelName, m_sparkVersion);
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
        modelHelper.saveModelMetadata(new NonClosableOutputStream.Zip(out), m_metaData);
    }

    /**
     * @return the {@link SparkVersion} this model was created with
     */
    public SparkVersion getSparkVersion() {
        return m_sparkVersion;
    }

    /**
     * @return the interpreter
     */
    public ModelInterpreter getInterpreter() {
        return m_interpreter;
    }

    /**
     * @return the spec
     */
    public SparkModelPortObjectSpec getSpec() {
        return new SparkModelPortObjectSpec(getSparkVersion(), getModelName());
    }

    /**
     * @return the unique model name
     */
    public String getModelName() {
        return m_interpreter.getModelName();
    }

    /**
     * @return the model
     */
    public Serializable getModel() {
        return m_model;
    }

    /**
     * @return additional meta data or <code>null</code>
     */
    public Serializable getMetaData() {
        return m_metaData;
    }

    /**
     * @return the tableSpec used to learn the mode including the class column if present
     * @see #getClassColumnName()
     */
    public DataTableSpec getTableSpec() {
        return m_tableSpec;
    }

    /**
     * @return the name of the class column or <code>null</code> if not needed
     */
    public String getClassColumnName() {
        return m_classColumnName;
    }

    /**
     * @return the name of all learning columns in the order they have been used during training
     */
    public List<String> getLearningColumnNames() {
        return getColNames(getClassColumnName());
    }

    /**
     * @return the name of all columns (learning and class columns)
     */
    public List<String> getAllColumnNames() {
        return getColNames(null);
    }

    /**
     * @return
     */
    private List<String> getColNames(final String filterColName) {
        final List<String> colNames = new ArrayList<>(m_tableSpec.getNumColumns());
        for (DataColumnSpec dataColumnSpec : m_tableSpec) {
            final String colName = dataColumnSpec.getName();
            if (!colName.equals(filterColName)) {
                colNames.add(colName);
            }
        }
        return colNames;
    }

    /**
     * @param inputSpec the {@link DataTableSpec} to find the learning columns
     * @return the index of each learning column in the order they should be presented to the model
     * @throws InvalidSettingsException if a column is not present or has an invalid data type
     */
    public Integer[] getLearningColumnIndices(final DataTableSpec inputSpec) throws InvalidSettingsException {
        final DataTableSpec learnerTabel = getTableSpec();
        final String classColumnName = getClassColumnName();
        final Integer[] colIdxs;
        if (classColumnName != null) {
            colIdxs = new Integer[learnerTabel.getNumColumns() - 1];
        } else {
            colIdxs = new Integer[learnerTabel.getNumColumns()];
        }
        int idx = 0;
        for (DataColumnSpec col : learnerTabel) {
            final String colName = col.getName();
            if (colName.equals(classColumnName)) {
                //skip the class column name
                continue;
            }
            final int colIdx = inputSpec.findColumnIndex(colName);
            if (colIdx < 0) {
                throw new InvalidSettingsException("Column with name " + colName + " not found in input data");
            }
            final DataType colType = inputSpec.getColumnSpec(colIdx).getType();
            if (!colType.equals(col.getType())) {
                throw new InvalidSettingsException("Column with name " + colName + " has incompatible type expected "
                    + col.getType() + " was " + colType);
            }
            colIdxs[idx++] = Integer.valueOf(colIdx);
        }
        return colIdxs;
    }

    /**
     * @return the summary of this model to use in the port tooltip
     */
    public String getSummary() {
        return m_interpreter.getSummary(this);
    }

    /**
     * @return the model views
     */
    public JComponent[] getViews() {
        return getInterpreter().getViews(this);
    }
}
