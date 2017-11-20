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

import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import com.knime.bigdata.spark.core.port.context.SparkContextPortObjectBase;

/**
 * Class required to load legacy workflows. Please use
 * {@link com.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec} instead.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@Deprecated
public class SparkContextPortObjectSpec extends com.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec {

    /**
     * Serializer used to save {@link SparkContextPortObject}s.
     */
    public static final class Serializer extends PortObjectSpec.PortObjectSpecSerializer<SparkContextPortObjectSpec> {
        @Override
        public void savePortObjectSpec(final SparkContextPortObjectSpec portObjectSpec,
            final PortObjectSpecZipOutputStream out) throws IOException {

            new com.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec.SpecSerializer()
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
