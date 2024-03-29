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
package org.knime.bigdata.spark.core.port.context;

import java.io.IOException;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkContextPortObjectSpec extends SparkContextPortObjectBase implements PortObjectSpec {

    /**
     * Creates a Spark context port object.
     * @param sparkContextID
     */
    public SparkContextPortObjectSpec(final SparkContextID sparkContextID) {
        super(sparkContextID);
    }

    /**
     * Serializer used to save {@link SparkContextPortObject}s.
     */
    public static final class SpecSerializer extends PortObjectSpec.PortObjectSpecSerializer<SparkContextPortObjectSpec> {
        @Override
        public void savePortObjectSpec(final SparkContextPortObjectSpec portObjectSpec,
            final PortObjectSpecZipOutputStream out)
            throws IOException {
            save(portObjectSpec.getContextID(), out);
        }
        @Override
        public SparkContextPortObjectSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in) throws IOException {
            final SparkContextID contextID = SparkContextPortObjectBase.load(in);
            return new SparkContextPortObjectSpec(contextID);
        }
    }
}
