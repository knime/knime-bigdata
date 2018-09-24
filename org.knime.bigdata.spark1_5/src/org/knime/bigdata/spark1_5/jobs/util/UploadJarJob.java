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

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.EmptyJobInput;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark1_5.api.NamedObjects;
import org.knime.bigdata.spark1_5.api.SparkJobWithFiles;
import org.knime.bigdata.spark1_5.jobs.scripting.java.JarRegistry;

/**
 *
 * @author Ole Ostergaard, KNIME GmbH, Konstanz, Germany
 */
@SparkClass
public class UploadJarJob implements SparkJobWithFiles<EmptyJobInput, EmptyJobOutput> {

    private static final Logger LOGGER = Logger.getLogger(UploadJarJob.class.getName());

    private static final long serialVersionUID = 1L;

    /**
     * {@inheritDoc}
     */
    @Override
    public EmptyJobOutput runJob(final SparkContext sparkContext, final EmptyJobInput input, final List<File> inputFiles, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception {
        JarRegistry.getInstance(sparkContext).ensureJarsAreLoaded(inputFiles);
        LOGGER.info("Ensured that list of Jars is loaded");
        return EmptyJobOutput.getInstance();
    }

}
