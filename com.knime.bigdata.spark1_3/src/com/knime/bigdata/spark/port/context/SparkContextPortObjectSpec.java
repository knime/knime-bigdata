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

import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkContextPortObjectSpec extends SparkContextPortObjectHelper implements PortObjectSpec {

    /**
     * Creates a Spark context port object.
     * @param context
     */
    public SparkContextPortObjectSpec(final KNIMESparkContext context) {
        super(context);
    }

    /**
     * Serializer used to save {@link SparkContextPortObjectSpec}s.
     * @return a new serializer
     */
    public static PortObjectSpecSerializer<SparkContextPortObjectSpec> getPortObjectSpecSerializer() {
        return new PortObjectSpecSerializer<SparkContextPortObjectSpec>() {
            @Override
            public void savePortObjectSpec(final SparkContextPortObjectSpec portObjectSpec,
                final PortObjectSpecZipOutputStream out)
                throws IOException {
                SparkContextPortObjectHelper.save(portObjectSpec.getContext(), out);
            }
            @Override
            public SparkContextPortObjectSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in) throws IOException {
                KNIMESparkContext context = SparkContextPortObjectHelper.load(in);
                return new SparkContextPortObjectSpec(context);
            }
        };
    }
}
