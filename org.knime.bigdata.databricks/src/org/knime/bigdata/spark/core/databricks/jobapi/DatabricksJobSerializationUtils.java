/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Apr 8, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.databricks.jobapi;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobData;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.WrapperJobOutput;
import org.knime.bigdata.spark.core.util.CustomClassLoadingObjectInputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

/**
 * Utilities to serialize job input and output data to staging area files.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class DatabricksJobSerializationUtils {

    private static final String KEY_ADDITIONAL_STAGING_FILES = "additionalStagingFiles";

    @SuppressWarnings("javadoc")
    public static interface StagingAreaAccess {
        public Entry<String, OutputStream> newUploadStream() throws IOException;

        public Entry<String, OutputStream> newUploadStream(String stagingFilename) throws IOException;

        public String uploadAdditionalFile(File fileToUpload, String stagingFilename) throws IOException;

        public InputStream newDownloadStream(String stagingFilename) throws IOException;

        public Path downloadToFile(InputStream in) throws IOException;

        public void deleteSafely(String stagingFilename);
    }

    private DatabricksJobSerializationUtils() {
    }

    /**
     * @return unique job identifier
     */
    public static String nextJobID() {
        return UUID.randomUUID().toString();
    }

    /**
     * Serialize job input and additional files to remote staging area.
     *
     * @param jobID unique job identifier
     * @param toSerialize job input to serialize
     * @param stagingAreaAccess remote staging area
     * @throws KNIMESparkException
     */
    public static void serializeJobInputToStagingFile(final String jobID, final DatabricksJobInput toSerialize,
        final StagingAreaAccess stagingAreaAccess) throws KNIMESparkException {

        serializeToStagingFile(jobID, "job-input", toSerialize, stagingAreaAccess);
    }

    /**
     * Serialize job output and additional files to remote staging area.
     *
     * @param jobID unique job identifier
     * @param toSerialize job input to serialize
     * @param stagingAreaAccess remote staging area
     * @throws KNIMESparkException
     */
    public static void serializeJobOutputToStagingFile(final String jobID, final WrapperJobOutput toSerialize,
        final StagingAreaAccess stagingAreaAccess) throws KNIMESparkException {

        serializeToStagingFile(jobID, "job-output", toSerialize, stagingAreaAccess);
    }

    /**
     * Serialize job input/output to remote staging file and return staging file key.
     *
     * @param jobID unique job identifier
     * @param postfix job-input or job-output
     * @param toSerialize job data to serialize
     * @param stagingAreaAccess remote staging area
     * @throws KNIMESparkException
     */
    private static <T extends JobData> void serializeToStagingFile(final String jobID, final String postfix,
        final T toSerialize, final StagingAreaAccess stagingAreaAccess) throws KNIMESparkException {

        final Map<String, Object> newInternalMap = new HashMap<>(toSerialize.getInternalMap());

        try {
            // upload any files attached to the job data to the staging area
            uploadAdditionalFilesToStagingArea(toSerialize.getFiles(), newInternalMap, stagingAreaAccess);

            // add serialized files to job input
            toSerialize.setInternalMap(newInternalMap);

            // serialize job data to the staging area
            serializeFieldsToStagingFile(jobID, postfix, toSerialize, stagingAreaAccess);

        } catch (IOException e) {
            throw new KNIMESparkException(e);
        }
    }

    /**
     * Serialize given map to a remote staging file.
     *
     * @param jobID unique job identifier
     * @param postfix job-input or job-output
     * @param fieldsToSerialize map with fields to serialize
     * @param stagingAreaAccess remote staging area
     * @return staging area file id (not path)
     * @throws IOException
     */
    private static String serializeFieldsToStagingFile(final String jobID, final String postfix,
        final JobData fieldsToSerialize, final StagingAreaAccess stagingAreaAccess) throws IOException {

        final String stagingFile = jobID + "-" + postfix;
        Entry<String, OutputStream> uploadStream = stagingAreaAccess.newUploadStream(stagingFile);
        try (final ObjectOutputStream out = new ObjectOutputStream(new SnappyOutputStream(uploadStream.getValue()))) {
            out.writeObject(fieldsToSerialize.getInternalMap());
            out.flush();
        }
        return uploadStream.getKey();
    }

    private static void uploadAdditionalFilesToStagingArea(final List<Path> filesToUpload,
        final Map<String, Object> toReturn, final StagingAreaAccess stagingAreaAccess) throws IOException {

        final List<String> additionalStagingFileNames = new LinkedList<>();
        toReturn.put(KEY_ADDITIONAL_STAGING_FILES, additionalStagingFileNames);
        for (Path localFile : filesToUpload) {
            Entry<String, OutputStream> uploadStream = stagingAreaAccess.newUploadStream();
            try (OutputStream out = new SnappyOutputStream(uploadStream.getValue())) {
                Files.copy(localFile, out);
            }
            additionalStagingFileNames.add(uploadStream.getKey());
        }
    }

    /**
     * Deserialize job input data from a staging file.
     *
     * @param jobID unique job ID
     * @param classLoader {@link ClassLoader} to use
     * @param stagingAreaAccess staging area to use
     * @return databricks job input
     * @throws KNIMESparkException
     */
    public static DatabricksJobInput deserializeJobInputFromStagingFile(final String jobID, final ClassLoader classLoader,
        final StagingAreaAccess stagingAreaAccess) throws KNIMESparkException {

        final DatabricksJobInput jobInput = new DatabricksJobInput();
        deserializeFromStagingFile(jobID, "job-input", jobInput, classLoader, stagingAreaAccess);
        return jobInput;
    }

    /**
     * Deserialize job output from a staging file.
     *
     * @param jobID unique job ID
     * @param classLoader {@link ClassLoader} to use
     * @param stagingAreaAccess staging area to use
     * @return databricks job output
     * @throws KNIMESparkException
     */
    public static WrapperJobOutput deserializeJobOutputFromStagingFile(final String jobID, final ClassLoader classLoader,
        final StagingAreaAccess stagingAreaAccess) throws KNIMESparkException {

        final WrapperJobOutput jobOutput = new WrapperJobOutput();
        deserializeFromStagingFile(jobID, "job-output", jobOutput, classLoader, stagingAreaAccess);
        return jobOutput;
    }

    private static <T extends JobData> void deserializeFromStagingFile(final String jobID, final String postfix,
        final T jobData, final ClassLoader classLoader, final StagingAreaAccess stagingAreaAccess)
        throws KNIMESparkException {

        try {
            final String stagingFile = jobID + "-" + postfix;
            deserializeFieldsFromStagingFile(stagingFile, jobData, classLoader, stagingAreaAccess);
            downloadAdditionalFilesFromStagingArea(jobData, stagingAreaAccess);
        } catch (IOException | ClassNotFoundException e) {
            throw new KNIMESparkException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends JobData> void deserializeFieldsFromStagingFile(final String stagingFileName,
        final T jobData, final ClassLoader classLoader, final StagingAreaAccess stagingAreaAccess) throws ClassNotFoundException, IOException {

        try (final ObjectInputStream in = new CustomClassLoadingObjectInputStream(
            new SnappyInputStream(stagingAreaAccess.newDownloadStream(stagingFileName)), classLoader)) {

            jobData.setInternalMap((Map<String, Object>)in.readObject());
        } finally {
            stagingAreaAccess.deleteSafely(stagingFileName);
        }
    }

    private static void downloadAdditionalFilesFromStagingArea(final JobData jobData, final StagingAreaAccess stagingAreaAccess)
        throws IOException {

        @SuppressWarnings("unchecked")
        final List<String> additionalStagingFileNames =
            (List<String>)jobData.getInternalMap().remove(KEY_ADDITIONAL_STAGING_FILES);

        for (String stagingFileName : additionalStagingFileNames) {
            try (final InputStream in = new SnappyInputStream(stagingAreaAccess.newDownloadStream(stagingFileName))) {
                final Path downloadedFile = stagingAreaAccess.downloadToFile(in);
                if (Files.isReadable(downloadedFile)) {
                    jobData.withFile(downloadedFile);
                } else {
                    throw new IOException("Cannot file download from staging area: " + stagingFileName);
                }

            } finally {
                stagingAreaAccess.deleteSafely(stagingFileName);
            }
        }
    }

}
