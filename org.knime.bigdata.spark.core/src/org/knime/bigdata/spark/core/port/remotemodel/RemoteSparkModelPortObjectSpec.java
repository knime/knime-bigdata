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
package org.knime.bigdata.spark.core.port.remotemodel;

import java.io.IOException;
import java.io.Serializable;
import java.util.zip.ZipEntry;

import javax.swing.JComponent;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.exception.MissingSparkModelHelperException;
import org.knime.bigdata.spark.core.model.ModelHelper;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectBase;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;
import org.knime.bigdata.spark.core.port.model.ModelHelperRegistry;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;

/**
 * Remote spark model port object specification, that extends a {@link SparkContextPortObjectSpec} with a
 * {@link SparkVersion}, a model name and meta data.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class RemoteSparkModelPortObjectSpec extends SparkContextPortObjectBase implements PortObjectSpec {

    private static final String ZIP_KEY_PORT_SPEC = "RemoteSparkModelPortObjectSpec";
    private static final String KEY_SPARK_VERSION = "SparkVersion";
    private static final String KEY_SPARK_MODEL = "SparkModel";
    private static final String ZIP_KEY_MODEL_META_DATA = "ModelMetaData";

    private final SparkVersion m_sparkVersion;
    private final String m_modelName;

    /** Optional meta data, might be <code>null</code> */
    private final Serializable m_modelMetaData;

    /**
     * Default constructor.
     *
     * @param sparkContextID
     * @param sparkVersion
     * @param modelName
     * @param modelMetaData readable and writable via {@link ModelHelper} or <code>null</code>
     */
    public RemoteSparkModelPortObjectSpec(final SparkContextID sparkContextID, final SparkVersion sparkVersion, final String modelName, final Serializable modelMetaData) {
        super(sparkContextID);
        m_sparkVersion = sparkVersion;
        m_modelName = modelName;
        m_modelMetaData = modelMetaData;
    }

    /** @return the sparkVersion */
    public SparkVersion getSparkVersion() {
        return m_sparkVersion;
    }

    /** @return unique model name */
    public String getModelName() {
        return m_modelName;
    }

    /** @return <code>true</code> if this spec has model meta data */
    public boolean hasModelMetadata() {
        return m_modelMetaData != null;
    }

    /** @return model meta data or <code>null</code> */
    public Serializable getModelMetaData() {
        return m_modelMetaData;
    }

    @Override
    public JComponent[] getViews() {
        return new JComponent[]{new RemoteSparkModelSpecView(this)};
    }

    /** port serializer */
    public static final class Serializer extends PortObjectSpec.PortObjectSpecSerializer<RemoteSparkModelPortObjectSpec> {
        @Override
        public void savePortObjectSpec(final RemoteSparkModelPortObjectSpec portObjectSpec,
            final PortObjectSpecZipOutputStream out) throws IOException {

            out.putNextEntry(new ZipEntry(ZIP_KEY_PORT_SPEC));
            final ModelContent config = new ModelContent(ZIP_KEY_PORT_SPEC);
            portObjectSpec.getContextID().saveToConfigWO(config);
            config.addString(KEY_SPARK_VERSION, portObjectSpec.getSparkVersion().toString());
            config.addString(KEY_SPARK_MODEL, portObjectSpec.getModelName());
            config.saveToXML(new NonClosableOutputStream.Zip(out));

            if (portObjectSpec.hasModelMetadata()) {
                try {
                    final ModelHelper modelHelper = ModelHelperRegistry.getModelHelper(portObjectSpec.getModelName(), portObjectSpec.getSparkVersion());
                    out.putNextEntry(new ZipEntry(ZIP_KEY_MODEL_META_DATA));
                    modelHelper.saveModelMetadata(new NonClosableOutputStream.Zip(out), portObjectSpec.getModelMetaData());
                } catch (MissingSparkModelHelperException e) {
                    throw new IOException(e);
                }
            }
        }

        @Override
        public RemoteSparkModelPortObjectSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in) throws IOException {
            try {
                ZipEntry ze = in.getNextEntry();
                if (!ze.getName().equals(ZIP_KEY_PORT_SPEC)) {
                    throw new IOException("Unexpected config key " + ze.getName() + ", expecting " + ZIP_KEY_PORT_SPEC + ".");
                }
                final ModelContentRO config = ModelContent.loadFromXML(new NonClosableInputStream.Zip(in));
                final SparkContextID sparkContextID = SparkContextID.fromConfigRO(config);
                final SparkVersion sparkVersion = SparkVersion.fromString(config.getString(KEY_SPARK_VERSION));
                final String modelName = config.getString(KEY_SPARK_MODEL);

                Serializable modelMetaData = null;
                ze = in.getNextEntry();
                if (ze != null && ze.getName().equals(ZIP_KEY_MODEL_META_DATA)) {
                    final ModelHelper modelHelper = ModelHelperRegistry.getModelHelper(modelName, sparkVersion);
                    modelMetaData = modelHelper.loadMetaData(new NonClosableInputStream.Zip(in));
                }

                return new RemoteSparkModelPortObjectSpec(sparkContextID, sparkVersion, modelName, modelMetaData);

            } catch(InvalidSettingsException | MissingSparkModelHelperException e) {
                throw new IOException("Failed to load config.", e);
            }
        }
    }
}
