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
 *   Created on 28.08.2018 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark3_1.base;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.eclipse.core.runtime.FileLocator;
import org.knime.bigdata.spark.node.scripting.python.util.DefaultPySparkHelper;
import org.knime.bigdata.spark3_1.api.Spark_3_1_CompatibilityChecker;
import org.knime.bigdata.spark3_1.jobs.scripting.python.PySparkDataExchanger;
import org.osgi.framework.FrameworkUtil;

/**
 * The PySpark helper class for Spark 3.1
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class Spark_3_1_PySparkHelper extends DefaultPySparkHelper {

    /**
     * Constructs a PySparkHelper for Spark 3.1
     */
    public Spark_3_1_PySparkHelper() {
        super(Spark_3_1_CompatibilityChecker.INSTANCE, PySparkDataExchanger.class.getCanonicalName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getLocalPySparkPath() throws IOException {
        final File sparkJarDir = new File(FileLocator.getBundleFile(FrameworkUtil.getBundle(PySparkDataExchanger.class)),
                "/lib");
        return createPySparkPath(sparkJarDir);
    }

}
