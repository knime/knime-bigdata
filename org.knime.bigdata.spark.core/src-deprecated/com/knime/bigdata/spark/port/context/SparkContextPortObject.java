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
 *   Created on Jun 29, 2016 by bjoern
 */
package com.knime.bigdata.spark.port.context;

import java.io.IOException;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.port.context.SparkContextPortObjectBase;

/**
 * Class required to load legacy workflows. Please use
 * {@link com.knime.bigdata.spark.core.port.context.SparkContextPortObject} instead.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@Deprecated
public class SparkContextPortObject extends com.knime.bigdata.spark.core.port.context.SparkContextPortObject {

    /**
     * Serializer used to save {@link SparkContextPortObject}s.
     */
    public static final class Serializer extends PortObjectSerializer<SparkContextPortObject> {
        /**
         * {@inheritDoc}
         */
        @Override
        public void savePortObject(final SparkContextPortObject portObject, final PortObjectZipOutputStream out,
            final ExecutionMonitor exec) throws IOException, CanceledExecutionException {

            new com.knime.bigdata.spark.core.port.context.SparkContextPortObject.ModelSerializer()
                .savePortObject(portObject, out, exec);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SparkContextPortObject loadPortObject(final PortObjectZipInputStream in, final PortObjectSpec spec,
            final ExecutionMonitor exec) throws IOException, CanceledExecutionException {

            final SparkContextID context = SparkContextPortObjectBase.load(in);
            return new SparkContextPortObject(context);
        }
    }

    /**
     * @param contextID
     */
    public SparkContextPortObject(final SparkContextID contextID) {
        super(contextID);
    }
}
