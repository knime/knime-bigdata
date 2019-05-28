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
 *   Created on May 26, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.port.model;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.zip.ZipEntry;

import javax.swing.JComponent;

import org.knime.bigdata.spark.core.exception.MissingSparkModelHelperException;
import org.knime.bigdata.spark.core.model.MLModelHelper;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.filestore.FileStore;
import org.knime.core.data.filestore.FileStorePortObject;
import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class SparkMLModelPortObject extends FileStorePortObject {

    private static final String ZIP_KEY_MODEL_META_DATA = "modelMetaData";

    private static final String ZIP_KEY_TABLE_SPEC = "tableSpec";

    private static final String KEY_TARGET_COLUMN_NAME = "targetColumnName";

    private static final String KEY_MODEL_NAME = "modelName";

    private static final String KEY_NAMED_MODEL_ID = "namedModelId";

    private static final String KEY_SPARK_VERSION = "sparkVersion";

    private static final String ZIP_KEY_ML_PIPELINE_MODEL_2_5_HEADER = "MLPipelineModel2_5";

    /**
     * Spark ML model port type.
     */
    public static final PortType PORT_TYPE = PortTypeRegistry.getInstance().getPortType(SparkMLModelPortObject.class);

    /**
     * Optional Spark ML port type.
     */
    public static final PortType PORT_TYPE_OPTIONAL =
        PortTypeRegistry.getInstance().getPortType(SparkMLModelPortObject.class, true);

    /**
     * The spec for this port object.
     */
    private final SparkModelPortObjectSpec m_spec;

    private final MLModel m_model;

    /**
     * @param model
     * @param zippedPipelineModel
     */
    public SparkMLModelPortObject(final MLModel model, final FileStore zippedPipelineModel) {
        super(Collections.singletonList(zippedPipelineModel));
        m_model = model;
        m_spec = model.getSpec();
    }

    /**
     * @param sparkVersion
     * @param modelName
     * @param namedModelId
     * @param tableSpec
     * @param targetColumnName
     * @param metaData
     * @throws MissingSparkModelHelperException
     */
    public SparkMLModelPortObject(final SparkVersion sparkVersion, final String modelName, final String namedModelId,
        final DataTableSpec tableSpec, final String targetColumnName, final Serializable metaData)
        throws MissingSparkModelHelperException {

        // we first create a placeholder MLModel with a null zippedModelPipeline, which we will add
        // later. At this point the file stores are not yet initialized.
        m_model = new MLModel(sparkVersion, modelName, null, namedModelId, tableSpec,
            targetColumnName, metaData);
        m_spec = m_model.getSpec();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkModelPortObjectSpec getSpec() {
        return m_spec;
    }

    /**
     * @return the model
     */
    public MLModel getModel() {
        if (m_model.getZippedPipelineModel() == null) {
            m_model.setZippedPipelineModel(getFileStore(0).getFile());
        }
        return m_model;
    }

    /**
     * Serializer used to save {@link SparkModelPortObject}s.
     */
    public static final class Serializer extends PortObjectSerializer<SparkMLModelPortObject> {
        /**
         * {@inheritDoc}
         */
        @SuppressWarnings("resource")
        @Override
        public void savePortObject(final SparkMLModelPortObject portObject, final PortObjectZipOutputStream out,
            final ExecutionMonitor exec) throws IOException, CanceledExecutionException {

            MLModel model = portObject.getModel();

            // write header
            out.putNextEntry(new ZipEntry(ZIP_KEY_ML_PIPELINE_MODEL_2_5_HEADER));
            NodeSettings header = new NodeSettings(ZIP_KEY_ML_PIPELINE_MODEL_2_5_HEADER);
            header.addString(KEY_SPARK_VERSION, model.getSparkVersion().toString());
            header.addString(KEY_MODEL_NAME, model.getModelName());
            header.addString(KEY_NAMED_MODEL_ID, model.getNamedModelId());
            header.addString(KEY_TARGET_COLUMN_NAME, model.getTargetColumnName());
            header.saveToXML(new NonClosableOutputStream.Zip(out));

            // write tab
            out.putNextEntry(new ZipEntry(ZIP_KEY_TABLE_SPEC));
            final NodeSettings tableSpec = new NodeSettings(ZIP_KEY_TABLE_SPEC);
            model.getTableSpec().save(tableSpec);
            tableSpec.saveToXML(new NonClosableOutputStream.Zip(out));

            final MLModelHelper<?> modelHelper;
            try {
                modelHelper = ModelHelperRegistry.getMLModelHelper(model.getModelName(), model.getSparkVersion());
            } catch (MissingSparkModelHelperException e) {
                throw new IOException(e);
            }

            // serialize the model meta data using a SparkModelHelper (which comes from a jobs plugin and hence has the classes
            // required for serialization
            out.putNextEntry(new ZipEntry(ZIP_KEY_MODEL_META_DATA));
            modelHelper.saveModelMetadata(new NonClosableOutputStream.Zip(out), model.getMetaData());
        }

        /**
         * {@inheritDoc}
         */
        @SuppressWarnings("resource")
        @Override
        public SparkMLModelPortObject loadPortObject(final PortObjectZipInputStream in, final PortObjectSpec spec,
            final ExecutionMonitor exec) throws IOException, CanceledExecutionException {

            try {
                // read the header
                in.getNextEntry();
                final NodeSettingsRO header = NodeSettings.loadFromXML(new NonClosableInputStream.Zip(in));
                SparkVersion sparkVersion = SparkVersion.fromString(header.getString(KEY_SPARK_VERSION));
                final String modelName = header.getString(KEY_MODEL_NAME);
                final String namedModelId = header.getString(KEY_NAMED_MODEL_ID);
                final String targetColumnName = header.getString(KEY_TARGET_COLUMN_NAME);

                final MLModelHelper<?> modelHelper = ModelHelperRegistry.getMLModelHelper(modelName, sparkVersion);

                // read the table spec
                in.getNextEntry();
                final DataTableSpec tableSpec =
                    DataTableSpec.load(NodeSettings.loadFromXML(new NonClosableInputStream.Zip(in)));

                in.getNextEntry();
                final Serializable metaData = modelHelper.loadMetaData(new NonClosableInputStream.Zip(in));

                return new SparkMLModelPortObject(sparkVersion, modelName, namedModelId, tableSpec, targetColumnName,
                    metaData);

            } catch (InvalidSettingsException | MissingSparkModelHelperException e) {
                throw new IOException(e);
            }

        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSummary() {
        return m_model.getSummary();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JComponent[] getViews() {
        return m_model.getViews();
    }
}
