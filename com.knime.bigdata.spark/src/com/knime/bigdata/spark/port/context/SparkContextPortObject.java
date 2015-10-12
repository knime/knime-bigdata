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
package com.knime.bigdata.spark.port.context;

import java.io.IOException;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkContextPortObject extends SparkContextPortObjectHelper implements PortObject {

    /**
     * Spark context port type.
     */
    public static final PortType TYPE = PortTypeRegistry.getInstance().getPortType(SparkContextPortObject.class);

    /**
     * Spark context type for optional ports.
     */
    public static final PortType TYPE_OPTIONAL =
        PortTypeRegistry.getInstance().getPortType(SparkContextPortObject.class, true);

    /**
     * Creates a Spark context port object.
     * @param context
     */
    public SparkContextPortObject(final KNIMESparkContext context) {
        super(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PortObjectSpec getSpec() {
        return new SparkContextPortObjectSpec(getContext());
    }

    /**
     * Serializer used to save {@link SparkContextPortObject}s.
     */
    public static final class ModelSerializer extends PortObjectSerializer<SparkContextPortObject> {
        /**
         * {@inheritDoc}
         */
        @Override
        public void savePortObject(final SparkContextPortObject portObject,
            final PortObjectZipOutputStream out, final ExecutionMonitor exec)
                    throws IOException, CanceledExecutionException {
            SparkContextPortObjectHelper.save(portObject.getContext(), out);
        }
        /**
         * {@inheritDoc}
         */
        @Override
        public SparkContextPortObject loadPortObject(final PortObjectZipInputStream in,
            final PortObjectSpec spec, final ExecutionMonitor exec)
                    throws IOException, CanceledExecutionException {
            final KNIMESparkContext context = SparkContextPortObjectHelper.load(in);
            return new SparkContextPortObject(context);
        }
    }
}
