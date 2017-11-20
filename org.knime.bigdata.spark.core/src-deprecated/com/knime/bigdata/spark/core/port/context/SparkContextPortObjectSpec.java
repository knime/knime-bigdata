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
package com.knime.bigdata.spark.core.port.context;

import java.io.IOException;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectBase;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;

import com.knime.bigdata.spark.port.context.SparkContextPortObject;

/**
 * Class required to load legacy workflows. Please use
 * {@link org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec} instead
 * @author Tobias Koetter, KNIME
 * @author Bjoern Lohrmann, KNIME
 * @deprecated  use {@link org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec} instead
 */
@Deprecated
public class SparkContextPortObjectSpec extends org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec {

    /**
     * Serializer used to save {@link SparkContextPortObject}s.
     */
    public static final class Serializer extends PortObjectSpec.PortObjectSpecSerializer<SparkContextPortObjectSpec> {
        @Override
        public void savePortObjectSpec(final SparkContextPortObjectSpec portObjectSpec,
            final PortObjectSpecZipOutputStream out) throws IOException {

            new org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec.SpecSerializer()
                .savePortObjectSpec(portObjectSpec, out);
        }

        @Override
        public SparkContextPortObjectSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in) throws IOException {
            final SparkContextID contextID = SparkContextPortObjectBase.load(in);
            return new SparkContextPortObjectSpec(contextID);
        }
    }

    /**
     * @param sparkContextID
     */
    public SparkContextPortObjectSpec(final SparkContextID sparkContextID) {
        super(sparkContextID);
    }
}
