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

import javax.swing.JComponent;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectBase;
import org.knime.bigdata.spark.core.port.model.SparkModel;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;

/**
 * Spark model that exists on cluster side.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class RemoteSparkModelPortObject extends SparkContextPortObjectBase implements PortObject {

    /** Spark remote model port type. */
    public static final PortType TYPE = PortTypeRegistry.getInstance().getPortType(RemoteSparkModelPortObject.class);

    private final RemoteSparkModelPortObjectSpec m_spec;

    private final SparkModel m_model;

    /**
     * Default constructor.
     * @param contextId
     * @param model
     */
    public RemoteSparkModelPortObject(final SparkContextID contextId, final SparkModel model) {
        super(contextId);
        m_spec = new RemoteSparkModelPortObjectSpec(contextId, model.getSparkVersion(), model.getModelName(), model.getMetaData());
        m_model = model;
    }

    @Override
    public RemoteSparkModelPortObjectSpec getSpec() {
        return m_spec;
    }

    /** @return remote model */
    public SparkModel getModel() {
        return m_model;
    }

    @Override
    public JComponent[] getViews() {
        return m_model.getViews();
    }

    /** Remote spark model port object serializer */
    public static final class Serializer extends PortObjectSerializer<RemoteSparkModelPortObject> {
        @Override
        public void savePortObject(final RemoteSparkModelPortObject portObject,
            final PortObjectZipOutputStream out, final ExecutionMonitor exec)
                    throws IOException, CanceledExecutionException {
            save(portObject.getContextID(), out);
            portObject.getModel().write(exec, out);
        }

        @Override
        public RemoteSparkModelPortObject loadPortObject(final PortObjectZipInputStream in,
            final PortObjectSpec spec, final ExecutionMonitor exec)
                    throws IOException, CanceledExecutionException {
            final SparkContextID contextID = SparkContextPortObjectBase.load(in);
            final SparkModel model = SparkModel.load(exec, in);
            return new RemoteSparkModelPortObject(contextID, model);
        }
    }

    @Override
    public String getSummary() {
        return String.format("Context: %s", getContextID().toPrettyString());
    }
}
