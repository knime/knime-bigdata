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
 *   Created on Jun 28, 2016 by bjoern
 */
package com.knime.bigdata.spark.port.data;

import java.io.IOException;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;

/**
 * Class required to load legacy workflows. Please use
 * {@link com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec} instead.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@Deprecated
public class SparkDataPortObjectSpec extends com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec {

    /**
     * @param contextID
     * @param spec
     */
    public SparkDataPortObjectSpec(final SparkContextID contextID, final DataTableSpec spec) {
        super(contextID, spec);
    }

    /**
     * A serializer for {@link SparkDataPortObjectSpec}s.
     *
     * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
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

            new com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec.Serializer()
                .savePortObjectSpec(portObjectSpec, out);
        }
    }
}
