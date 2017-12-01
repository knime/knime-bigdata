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
 *   Created on Apr 15, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.job;

import java.io.File;
import java.util.List;

import org.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;

/**
 *
 * @author Bjoern Lohrmann, KNIME.com
 * @param <I> {@link JobInput}
 * @param <O> {@link JobOutput}
 */
public interface JobWithFilesRunFactory<I extends JobInput, O extends JobOutput> extends JobRunFactory<I, O> {

    /**
     * @param input {@link JobInput}
     * @param localFiles {@link List} of local files to send with the job
     * @return the {@link JobWithFilesRun} instance
     */
    public JobWithFilesRun<I, O> createRun(I input, List<File> localFiles);

    /**
     * @return the lifetime of uploaded files, i.e. how long the remain on the remote side.
     */
    public FileLifetime getFilesLifetime();

    /**
     * @return whether to use the file input cache or not
     */
    public boolean useInputFileCopyCache();

}
