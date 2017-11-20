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
 *   Created on 28.01.2016 by koetter
 */
package org.knime.bigdata.spark.core.job;

import java.io.File;
import java.util.List;

import org.knime.bigdata.spark.core.context.SparkContext;

/**
 * This interface describes a Spark job run that requires input files. These input files are expected to be provided by
 * KNIME nodes. The local {@link SparkContext} will automatically copy them to the Spark-side context, where the Spark
 * driver program runs. On the Spark-side, the job will be automatically provided with the Spark-side copies of the
 * input files. Moreover, this interface allows to specify the <em>lifetime</em> of input files, and whether a
 * <em>input file copy cache</em> shall be used.
 *
 * <p>
 * The {@link FileLifetime} returned by {@link #getInputFilesLifetime()} specifies when the Spark-side copies of the
 * input files can be automatically deleted.
 * </p>
 *
 * *
 * <p>
 * The flag returned by {@link #useInputFileCopyCache()} specifies whether the local {@link SparkContext} should only
 * copy previously unseen input files to the spark-side context. If two subsequent job runs have the flag set to true
 * and specify one or more identical input files, the local {@link SparkContext} will detect this and only copy the
 * input files once. Input files of two subsequent job runs are considered identical, if the file system paths are
 * identical and if the modification time has not changed. Note this optimization is only possible if
 * {@link #getInputFilesLifetime()} returns {@link FileLifetime#CONTEXT}, otherwise the Spark-side copies are deleted
 * after each job run.
 * </p>
 *
 *
 * @author Tobias Koetter, KNIME.com
 * @author Bjoern Lohrmann, KNIME.com
 *
 * @param <I> the {@link JobInput} object
 * @param <O> the {@link JobOutput} object
 */
public interface JobWithFilesRun<I extends JobInput, O extends JobOutput> extends JobRun<I, O> {

    /**
     * When running a Spark job with files, we need to know when these files (actually: their Spark-side copies) can be
     * cleaned up. This enum is used to indicate when the Spark-side copies can be deleted.
     *
     */
    public enum FileLifetime {
            /**
             * Spark-side copies can be deleted after the job has executed.
             */
        JOB,

            /**
             * Spark-side copies can be deleted when the Spark context is destroyed.
             */
        CONTEXT,

            /**
             * Spark-side copies should never be deleted (use with caution).
             */
        NEVER;
    }

    /**
     * The files returned by this will be automatically copied to the Spark-side context (where the Spark driver runs).
     * There, the Spark job will be automatically provided with the Spark-side copies of the input files.
     *
     * @return a list of files, that must be readable via the file system local to the client (i.e. KNIME Analytics
     *         Platform).
     */
    List<File> getInputFiles();

    /**
     *
     * @return the {@link FileLifetime} that specifies when the Spark-side copies of the input files can be deleted.
     */
    FileLifetime getInputFilesLifetime();

    /**
     * If true, the local {@link SparkContext} should only copy previously unseen input files to the spark-side context.
     * The input files of a job run are considered as "previously seen", if there has been a prior job run with an input
     * file of the same file system path and modification time and which also used the input file copy cache.
     *
     * @return true if the client-side {@link SparkContext} should only copy input files it has not previously seen to
     *         the spark-side context.
     */
    boolean useInputFileCopyCache();
}
