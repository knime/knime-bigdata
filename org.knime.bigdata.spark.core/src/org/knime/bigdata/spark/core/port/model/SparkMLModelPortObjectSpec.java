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

import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class SparkMLModelPortObjectSpec extends SparkModelPortObjectSpec {

    public SparkMLModelPortObjectSpec(final SparkVersion version, final String modelName) {
        super(version, modelName);
    }

    /**
     * @param model
     */
    public SparkMLModelPortObjectSpec(final ModelContentRO model) {
        super(model);
    }

    /**
     * A serializer for {@link SparkModelPortObjectSpec}s.
     *
     * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
     */
    public static final class Serializer extends PortObjectSpecSerializer<SparkMLModelPortObjectSpec> {
        @Override
        public SparkMLModelPortObjectSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in) throws IOException {
            return new SparkMLModelPortObjectSpec(SparkModelPortObjectSpec.Serializer.loadModelContent(in));
        }

        @Override
        public void savePortObjectSpec(final SparkMLModelPortObjectSpec portObjectSpec,
            final PortObjectSpecZipOutputStream out) throws IOException {
            SparkModelPortObjectSpec.Serializer.saveModelContent(out, portObjectSpec);
        }
    }
}
