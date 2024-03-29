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
package com.knime.bigdata.spark.core.port.data;

import java.io.IOException;

import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;

/**
 * Class required to load legacy workflows. Please use
 * {@link org.knime.bigdata.spark.core.port.data.SparkDataPortObject} instead.
 *
 * @author Tobias Koetter, KNIME
 * @author Bjoern Lohrmann, KNIME
 * @deprecated use {@link org.knime.bigdata.spark.core.port.data.SparkDataPortObject} instead.
 */
@Deprecated
public class SparkDataPortObject extends org.knime.bigdata.spark.core.port.data.SparkDataPortObject {

    /**
     * @param data
     */
    public SparkDataPortObject(final SparkDataTable data) {
        super(data);
    }


    /**
     * Serializer used to save {@link SparkDataPortObject}s.
     */
    public static final class Serializer extends PortObjectSerializer<SparkDataPortObject> {
        /**
         * {@inheritDoc}
         */
        @Override
        public void savePortObject(final SparkDataPortObject portObject,
            final PortObjectZipOutputStream out, final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {

            new org.knime.bigdata.spark.core.port.data.SparkDataPortObject.Serializer().savePortObject(portObject, out,
                exec);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SparkDataPortObject loadPortObject(final PortObjectZipInputStream in,
            final PortObjectSpec spec, final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
            return new SparkDataPortObject(new SparkDataTable(in));
        }
    }
}
