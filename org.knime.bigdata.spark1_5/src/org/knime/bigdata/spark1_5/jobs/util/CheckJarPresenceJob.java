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
 *   Created on May 4, 2018 by oole
 */
package org.knime.bigdata.spark1_5.jobs.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.util.jarupload.CheckJarPresenceJobInput;
import org.knime.bigdata.spark.core.util.jarupload.CheckJarPresenceJobOutput;
import org.knime.bigdata.spark1_5.api.NamedObjects;
import org.knime.bigdata.spark1_5.api.SparkJob;

/**
 * Job to check for the presence of certain jars in the Spark classpath. The actual check is carried out by trying to
 * load classes which are usually contained with those jars.
 *
 * @author Ole Ostergaard, KNIME GmbH, Konstanz, Germany
 */
@SparkClass
public class CheckJarPresenceJob implements SparkJob<CheckJarPresenceJobInput, CheckJarPresenceJobOutput> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(CheckJarPresenceJob.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public CheckJarPresenceJobOutput runJob(final SparkContext sparkContext, final CheckJarPresenceJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {

        final List<String> jarClassList = input.getJarClassList();
        final List<String> missingJarClasses = new ArrayList<>();

        for (String jarClass : jarClassList) {
            try {
                getClass().getClassLoader().loadClass(jarClass);
            } catch (ClassNotFoundException e) {
                LOGGER.info("Class " + jarClass + " cannot be found.");
                missingJarClasses.add(jarClass);
            }
        }
        return new CheckJarPresenceJobOutput(missingJarClasses);
    }
}
