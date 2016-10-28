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
 *   Created on Oct 17, 2016 by Sascha Wolke, KNIME.com
 */
package com.knime.bigdata.spark1_5.jobs.genericdatasource;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;

import com.google.common.collect.ImmutableMap;

/**
 * Generic data sources driver jar provider.
 * @author Sascha Wolke, KNIME.com
 */
public class GenericDataSourceDriverUtil {
    private final static Map<String, String[]> DRIVER_JARS = ImmutableMap.of(
        "com.databricks.spark.avro", new String[] { "lib/databricks-spark-avro_2.10-2.0.1.jar" },
        "com.databricks.spark.csv", new String[] { "lib/apache-commons-csv-1.1.jar", "lib/databricks-spark-csv_2.10-1.5.0.jar" }
    );

    /**
     * Returns a list bundled jars for given format.
     * @param format - DataSources format
     * @return List contains bundled jar files or empty list.
     */
    public static List<File> getBundledJars(final String format) {
        if (DRIVER_JARS.containsKey(format)) {
            Bundle bundle = FrameworkUtil.getBundle(GenericDataSourceDriverUtil.class);
            String jarNames[] = DRIVER_JARS.get(format);
            List<File> bundledJars = new ArrayList<File>(jarNames.length);

            for (String name : jarNames) {
                URL url = FileLocator.find(bundle, new Path(name), null);
                if (url == null) {
                    throw new RuntimeException("Unable to find bundled jar: " + name);
                }

                try {
                    bundledJars.add(new File(FileLocator.toFileURL(url).getPath()));
                } catch (IOException e) {
                    throw new RuntimeException("Unable to find bundled jar: " + name);
                }
            }

            return bundledJars;

        } else {
            return new ArrayList<File>();
        }
    }
}
