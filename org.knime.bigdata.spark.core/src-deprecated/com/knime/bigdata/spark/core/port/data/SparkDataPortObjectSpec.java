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

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;

/**
 * Class required to load legacy workflows. Please use
 * {@link org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec} instead.
 *
 * @author Tobias Koetter, KNIME
 * @author Bjoern Lohrmann, KNIME
 * @deprecated use {@link org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec} instead.
 */
@Deprecated
public class SparkDataPortObjectSpec extends org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec {

    /**
     * Creates a new instance bound with the given data table spec and which is bound to the given Spark context.
     *
     * @param contextID The ID of the underlying Spark context.
     * @param spec The {@link DataTableSpec} of the respective Spark data table.
     */
    public SparkDataPortObjectSpec(final SparkContextID contextID, final DataTableSpec spec) {
        super(contextID, spec);
    }

    /**
     * A serializer for {@link SparkDataPortObjectSpec}s.
     *
     * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
     */
    public static final class Serializer extends PortObjectSpecSerializer<SparkDataPortObjectSpec> {
        @Override
        public SparkDataPortObjectSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in)
            throws IOException {

            final SparkDataTable table = new SparkDataTable(in);
            return new SparkDataPortObjectSpec(table.getContextID(), table.getTableSpec());
        }

        @Override
        public void savePortObjectSpec(final SparkDataPortObjectSpec portObjectSpec,
            final PortObjectSpecZipOutputStream out) throws IOException {

            new org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec.Serializer()
                .savePortObjectSpec(portObjectSpec, out);
        }
    }
}
