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

import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;

/**
 * Class required to load legacy workflows. Please use
 * {@link org.knime.bigdata.spark.core.port.model.SparkModelPortObjectSpec} instead.
 * @author Tobias Koetter, KNIME
 * @author Bjoern Lohrmann, KNIME
 * @deprecated use {@link org.knime.bigdata.spark.core.port.model.SparkModelPortObjectSpec} instead.
 */
@Deprecated
public class SparkModelPortObjectSpec extends org.knime.bigdata.spark.core.port.model.SparkModelPortObjectSpec {

    /**
     * A serializer for {@link SparkModelPortObjectSpec}s.
     *
     * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
     */
    public static final class Serializer extends PortObjectSpecSerializer<SparkModelPortObjectSpec> {

        @Override
        public SparkModelPortObjectSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in) throws IOException {

            org.knime.bigdata.spark.core.port.model.SparkModelPortObjectSpec other =
                new org.knime.bigdata.spark.core.port.model.SparkModelPortObjectSpec.Serializer()
                    .loadPortObjectSpec(in);

            return new SparkModelPortObjectSpec(other.getSparkVersion(), other.getModelName());
        }

        @Override
        public void savePortObjectSpec(final SparkModelPortObjectSpec portObjectSpec,
            final PortObjectSpecZipOutputStream out) throws IOException {

            new org.knime.bigdata.spark.core.port.model.SparkModelPortObjectSpec.Serializer()
                .savePortObjectSpec(portObjectSpec, out);
        }
    }

    /**
     * @param version
     * @param type
     */
    public SparkModelPortObjectSpec(final SparkVersion version, final String type) {
        super(version, type);
    }
}
