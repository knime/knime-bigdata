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
 *   Created on Apr 8, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.livy.jobapi;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.log4j.Logger;
import org.knime.bigdata.spark.core.context.util.Base64SerializationUtils;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobData;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.util.CustomClassLoadingObjectInputStream;
import org.knime.core.util.FileUtil;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

/**
 * {@LivyClient} uses a Kryo serializer to (de)serialize job results. The amount of classes available to the Kryo
 * deserializer that deserializes a job result, are more limited than those available to the job. Unfortunately the Livy
 * API does not provide a way to configure a classloader for the serializer. This class offers methods to make job
 * results safe for deserialization on the client.
 *
 * @author Bjoern Lohrmann, KNIME.COM
 */
@SparkClass
public class LivyJobSerializationUtils {
    
    private static final String KEY_FIELDS_SERIALIZED_TO_STAGING_FILE = "fieldsSerializedToStagingFile";

    private static final String KEY_FIELDS_SERIALIZED_TO_BASE64 = "fieldsSerializedToBase64";

    private static final String KEY_SERIALIZED_FIELDS_STAGING_FILE = "serializedFieldsStagingfile";

    private static final String KEY_ADDITIONAL_STAGING_FILES = "additionalStagingFiles";

    public static interface StagingAreaAccess {
        public Entry<String, OutputStream> newUploadStream() throws IOException;

        public InputStream newDownloadStream(String stagingFilename) throws IOException;

        public Path downloadToFile(InputStream in) throws IOException;

        public void deleteSafely(String stagingFilename);
    }

    private LivyJobSerializationUtils() {
    }

    public static <T extends JobData> T preKryoSerialize(final T toSerialize, final StagingAreaAccess stagingAreaAccess,
        final T toReturn) throws KNIMESparkException {

        final List<String> fieldsToSerializeToStagingFile = new LinkedList<>();
        final List<String> fieldsToSerializeToBase64 = new LinkedList<>();

        determineFieldsToSerialize(toSerialize, fieldsToSerializeToStagingFile, fieldsToSerializeToBase64);

        final Map<String, Object> newInternalMap = new HashMap<>(toSerialize.getInternalMap());

        try {
            // perform pre-serialization to base64 strings for selected fields
            serializeFieldsToBase64(fieldsToSerializeToBase64, newInternalMap);

            // perform pre-serialization to a staging file for selected fields
            serializeFieldsToStagingFile(fieldsToSerializeToStagingFile, newInternalMap, stagingAreaAccess);

            // upload any files attached to the job data to the staging area
            uploadAdditionalFilesToStagingArea(toSerialize.getFiles(), newInternalMap, stagingAreaAccess);
        } catch (IOException e) {
            throw new KNIMESparkException(e);
        }

        toReturn.setInternalMap(newInternalMap);

        return toReturn;
    }

    private static void determineFieldsToSerialize(final JobData toSerialize,
        final List<String> fieldsToSerializeToStagingfile, final List<String> fieldsToSerializeToBase64) {
        
        for (Entry<String, Object> entry : toSerialize.getInternalMap().entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();

            if (value != null) {
                final Class<?> valueClass = value.getClass();

                // we don't do any pre-kryo serialization for the Java base types
                if (valueClass.getCanonicalName().startsWith("java.lang")) {
                    continue;
                }

                // if value is a throwable, list, or map we pre-serialize to base64 strings,
                // otherwise we pre-serialize to a staging file
                if (Throwable.class.isAssignableFrom(valueClass) || List.class.isAssignableFrom(valueClass)
                    || Map.class.isAssignableFrom(valueClass)) {
                    fieldsToSerializeToBase64.add(key);
                } else {
                    fieldsToSerializeToStagingfile.add(key);
                }

                assert valueClass.getCanonicalName().startsWith("java.") || valueClass.getCanonicalName().startsWith("org.knime.") //
                    || valueClass.isArray() //
                    : "Found serialized field " + key + " with unknown value class: " + valueClass.getCanonicalName();
            }
        }
    }

    private static void serializeFieldsToBase64(final List<String> fieldsToSerializeToBase64,
        final Map<String, Object> toReturn) {
        for (final String fieldToSerialize : fieldsToSerializeToBase64) {
            toReturn.put(fieldToSerialize, Base64SerializationUtils.serializeToBase64(toReturn.get(fieldToSerialize)));
        }
        toReturn.put(KEY_FIELDS_SERIALIZED_TO_BASE64, fieldsToSerializeToBase64);
    }

    private static void serializeFieldsToStagingFile(final List<String> fieldsToSerializeToStagingfile,
        final Map<String, Object> toReturn, StagingAreaAccess stagingAreaAccess) throws IOException {

        toReturn.put(KEY_FIELDS_SERIALIZED_TO_STAGING_FILE, fieldsToSerializeToStagingfile);
        if (!fieldsToSerializeToStagingfile.isEmpty()) {
            Entry<String, OutputStream> uploadStream = stagingAreaAccess.newUploadStream();

            try (final ObjectOutputStream out =
                new ObjectOutputStream(new SnappyOutputStream(uploadStream.getValue()))) {
                for (final String fieldToSerialize : fieldsToSerializeToStagingfile) {
                    out.writeObject(toReturn.remove(fieldToSerialize));
                }
            }
            toReturn.put(KEY_SERIALIZED_FIELDS_STAGING_FILE, uploadStream.getKey());
        }
    }

    private static void uploadAdditionalFilesToStagingArea(final List<Path> filesToUpload,
        final Map<String, Object> toReturn, StagingAreaAccess stagingAreaAccess) throws IOException {

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
     * Post-processes the given map after deserialization with Kryo. The map is assumed to have been created with the
     * {@link #preKryoSerialize(Map)} method.
     *
     * @param toPostProcess A map to post-process after deserialization.
     * @param classLoader A classloader with which to load classes for the deserialized values.
     * @return a new map with fully deserialized values
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public static <T extends JobData> T postKryoDeserialize(final T jobData, final ClassLoader classLoader,
        final StagingAreaAccess stagingAreaAccess) throws KNIMESparkException {

        final Map<String, Object> deserializedInternalMap = new HashMap<>(jobData.getInternalMap());

        try {
            deserializeFieldsFromBase64(deserializedInternalMap, classLoader);
            deserializeFieldsFromStagingFile(deserializedInternalMap, classLoader, stagingAreaAccess);

            jobData.setInternalMap(deserializedInternalMap);

            downloadAdditionalFilesFromStagingArea(jobData, stagingAreaAccess);
        } catch (IOException | ClassNotFoundException e) {
            throw new KNIMESparkException(e);
        }

        return jobData;
    }

    @SuppressWarnings("unchecked")
    private static void deserializeFieldsFromBase64(Map<String, Object> deserializedInternalMap,
        ClassLoader classLoader) throws ClassNotFoundException, IOException {

        for (String serializedField : (List<String>)deserializedInternalMap.remove(KEY_FIELDS_SERIALIZED_TO_BASE64)) {
            Object deserializedValue = Base64SerializationUtils
                .deserializeFromBase64((String)deserializedInternalMap.get(serializedField), classLoader);
            deserializedInternalMap.put(serializedField, deserializedValue);

            final String valueClass = deserializedValue.getClass().getCanonicalName();
            assert valueClass.startsWith("java.") || valueClass.startsWith("org.knime.") //
                : "Found serialized field " + serializedField + " with unknown value class: " + valueClass;
        }

    }

    @SuppressWarnings("unchecked")
    private static void deserializeFieldsFromStagingFile(Map<String, Object> deserializedInternalMap,
        ClassLoader classLoader, StagingAreaAccess stagingAreaAccess) throws ClassNotFoundException, IOException {

        final List<String> serializedFields =
            (List<String>)deserializedInternalMap.remove(KEY_FIELDS_SERIALIZED_TO_STAGING_FILE);

        if (!serializedFields.isEmpty()) {
            final String stagingFileName = (String)deserializedInternalMap.remove(KEY_SERIALIZED_FIELDS_STAGING_FILE);

            try (final ObjectInputStream in = new CustomClassLoadingObjectInputStream(
                new SnappyInputStream(stagingAreaAccess.newDownloadStream(stagingFileName)), classLoader)) {

                for (String serializedField : serializedFields) {
                    Object deserializedValue = in.readObject();
                    deserializedInternalMap.put(serializedField, deserializedValue);
                }
            } finally {
                stagingAreaAccess.deleteSafely(stagingFileName);
            }
        }
    }

    private static void downloadAdditionalFilesFromStagingArea(JobData jobData, StagingAreaAccess stagingAreaAccess)
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
