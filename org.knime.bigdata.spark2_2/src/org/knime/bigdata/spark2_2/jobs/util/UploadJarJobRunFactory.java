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
 *   Created on 29.01.2016 by koetter
 */
package org.knime.bigdata.spark2_2.jobs.util;

import org.knime.bigdata.spark.core.job.DefaultJobWithFilesRunFactory;
import org.knime.bigdata.spark.core.job.EmptyJobInput;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import org.knime.bigdata.spark.core.util.jarupload.EnsureJarPresence;

/**
 *
 * @author Ole Ostergaard, KNIME GmbH, Konstanz, Germany
 */
public class UploadJarJobRunFactory extends DefaultJobWithFilesRunFactory<EmptyJobInput, EmptyJobOutput> {

    /** Constructor. */
    public UploadJarJobRunFactory() {
        super(EnsureJarPresence.UPLOAD_JAR_JOB_ID, UploadJarJob.class,
            EmptyJobOutput.class, FileLifetime.CONTEXT, true);
    }
}
