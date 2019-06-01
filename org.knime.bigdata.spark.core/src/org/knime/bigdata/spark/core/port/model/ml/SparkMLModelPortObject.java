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
package org.knime.bigdata.spark.core.port.model.ml;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.zip.ZipEntry;

import javax.swing.JComponent;

import org.knime.bigdata.spark.core.exception.MissingSparkModelHelperException;
import org.knime.bigdata.spark.core.port.model.SparkModelPortObject;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.filestore.FileStore;
import org.knime.core.data.filestore.FileStorePortObject;
import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
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

    private static final String KEY_TABLE_SPEC = "tableSpec";

    private static final String KEY_TARGET_COLUMN_NAME = "targetColumnName";

    private static final String KEY_MODEL_NAME = "modelName";

    private static final String KEY_NAMED_MODEL_ID = "namedModelId";

    private static final String KEY_SPARK_VERSION = "sparkVersion";

    private static final String ZIP_KEY_ML_MODEL = "MLModel";

    private static final String KEY_MODEL_META_DATA = "modelMetaData";

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
    private final SparkMLModelPortObjectSpec m_spec;

    private final MLModel m_model;

    /**
     * @param model
     * @param zippedPipelineModel
     */
    public SparkMLModelPortObject(final MLModel model, final FileStore zippedPipelineModel) {
        super(Collections.singletonList(zippedPipelineModel));
        m_model = model;
        m_spec = new SparkMLModelPortObjectSpec(model.getSparkVersion(), model.getModelName(), model.getTableSpec(),
            model.getTargetColumnName().orElse(null));
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
        final DataTableSpec tableSpec, final String targetColumnName, final MLMetaData metaData)
        throws MissingSparkModelHelperException {

        // we first create a placeholder MLModel with a null zippedModelPipeline, which we will add
        // later. At this point the file stores are not yet initialized.
        m_model = new MLModel(sparkVersion, modelName, null, namedModelId, tableSpec, targetColumnName, metaData);
        m_spec = new SparkMLModelPortObjectSpec(m_model.getSparkVersion(), m_model.getModelName(), tableSpec,
            m_model.getTargetColumnName().orElse(null));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkMLModelPortObjectSpec getSpec() {
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
            out.putNextEntry(new ZipEntry(ZIP_KEY_ML_MODEL));
            ModelContent modelContent = new ModelContent(ZIP_KEY_ML_MODEL);
            modelContent.addString(KEY_SPARK_VERSION, model.getSparkVersion().toString());
            modelContent.addString(KEY_MODEL_NAME, model.getModelName());
            modelContent.addString(KEY_NAMED_MODEL_ID, model.getNamedModelId());
            modelContent.addString(KEY_TARGET_COLUMN_NAME, model.getTargetColumnName().orElse(null));

            final ModelContentWO tableSpecModel = modelContent.addModelContent(KEY_TABLE_SPEC);
            model.getTableSpec().save(tableSpecModel);

            final Optional<MLMetaData> modelMetaData = model.getModelMetaData(MLMetaData.class);
            if (modelMetaData.isPresent()) {
                final ModelContentWO metaDataContent = modelContent.addModelContent(KEY_MODEL_META_DATA);
                MLMetaDataUtils.saveToModelContent(modelMetaData.get(), metaDataContent);
            }

            modelContent.saveToXML(new NonClosableOutputStream.Zip(out));
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
                final ModelContentRO modelContent = ModelContent.loadFromXML(new NonClosableInputStream.Zip(in));

                final SparkVersion sparkVersion = SparkVersion.fromString(modelContent.getString(KEY_SPARK_VERSION));
                final String modelName = modelContent.getString(KEY_MODEL_NAME);
                final String namedModelId = modelContent.getString(KEY_NAMED_MODEL_ID);
                final String targetColumnName = modelContent.getString(KEY_TARGET_COLUMN_NAME);
                final DataTableSpec tableSpec = DataTableSpec.load(modelContent.getModelContent(KEY_TABLE_SPEC));

                MLMetaData metaData = null;
                if (modelContent.containsKey(KEY_MODEL_META_DATA)) {
                    metaData = MLMetaDataUtils.loadFromModelContent(modelContent.getModelContent(KEY_MODEL_META_DATA));
                }

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
