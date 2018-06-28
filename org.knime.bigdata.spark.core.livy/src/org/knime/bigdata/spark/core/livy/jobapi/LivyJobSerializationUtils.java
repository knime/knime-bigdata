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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;

import org.knime.bigdata.spark.core.context.util.Base64SerializationUtils;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.util.CustomClassLoadingObjectInputStream;

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

    public static final String KEY_SERIALIZED_FIELDS_STAGING_FILE = "serializedFieldsStagingfile";

    private LivyJobSerializationUtils() {
    }

    /**
     * Pre-processes the given map for serialization with Kryo. This method Base64-serializes every value in the map
     * that is not null, a primitive or a String.
     *
     * @param mapToPreprocess A map to prepare for serialization.
     * @return a new map that can be safely serialized and deserialized with Kryo
     */
    public static Map<String, Object> preKryoSerialize(final Map<String, Object> mapToPreprocess) {
        final List<String> serializedFields = new LinkedList<>();

        final Map<String, Object> toReturn = new HashMap<>(mapToPreprocess);

        for (Entry<String, Object> entry : mapToPreprocess.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();

            if (value != null) {
                final Class<?> valueClass = value.getClass();
                if (!(valueClass.isPrimitive() || valueClass == String.class)) {
                    serializedFields.add(key);
                    toReturn.put(key, Base64SerializationUtils.serializeToBase64(value));
                }
            }
        }

        // serialized fields is guaranteed to be still free (by convention)
        toReturn.put(KEY_FIELDS_SERIALIZED_TO_STAGING_FILE, serializedFields);

        return toReturn;
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
    @SuppressWarnings("unchecked")
    public static Map<String, Object> postKryoDeserialize(final Map<String, Object> toPostProcess,
        final ClassLoader classLoader) throws ClassNotFoundException, IOException {

        final Map<String, Object> toReturn = new HashMap<>(toPostProcess);
        final List<String> serializedFields = (List<String>)toPostProcess.get(KEY_FIELDS_SERIALIZED_TO_STAGING_FILE);

        for (String serializedField : serializedFields) {
            Object deserializedValue =
                Base64SerializationUtils.deserializeFromBase64((String)toPostProcess.get(serializedField), classLoader);
            toReturn.put(serializedField, deserializedValue);
        }

        return toReturn;
    }

    public static Map<String, Object> serializeObjectsToStream(final Map<String, Object> toSerialize,
        final Supplier<OutputStream> outStream) throws IOException {

        final List<String> fieldsToSerializeToStagingfile = new LinkedList<>();
        final List<String> fieldsToSerializeToBase64 = new LinkedList<>();
        final Map<String, Object> toReturn = new HashMap<>(toSerialize);

        for (Entry<String, Object> entry : toSerialize.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();

            if (value != null) {
                final Class<?> valueClass = value.getClass();
                if (!(valueClass.isPrimitive() || valueClass == String.class)) {
                    if (Throwable.class.isAssignableFrom(valueClass)) {
                        fieldsToSerializeToBase64.add(key);
                    } else {
                        fieldsToSerializeToStagingfile.add(key);
                    }
                }
            }
        }

        for (final String fieldToSerialize : fieldsToSerializeToBase64) {
            toReturn.put(fieldToSerialize, Base64SerializationUtils.serializeToBase64(toReturn.get(fieldToSerialize)));
        }
        toReturn.put(KEY_FIELDS_SERIALIZED_TO_BASE64, fieldsToSerializeToBase64);

        // serialized fields is guaranteed to be still free (by convention)
        toReturn.put(KEY_FIELDS_SERIALIZED_TO_STAGING_FILE, fieldsToSerializeToStagingfile);
        if (!KEY_FIELDS_SERIALIZED_TO_STAGING_FILE.isEmpty()) {
            try (final ObjectOutputStream out = new ObjectOutputStream(outStream.get())) {
                for (final String fieldToSerialize : fieldsToSerializeToStagingfile) {
                    out.writeObject(toReturn.remove(fieldToSerialize));
                }
            }
        }
        return toReturn;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> deserializeObjectsFromStream(final Map<String, Object> toDeserialize,
        final ClassLoader classLoader, final Supplier<InputStream> inStream)
        throws ClassNotFoundException, IOException {

        final Map<String, Object> toReturn = new HashMap<>(toDeserialize);

        for (String serializedField : (List<String>)toReturn.remove(KEY_FIELDS_SERIALIZED_TO_BASE64)) {
            Object deserializedValue =
                Base64SerializationUtils.deserializeFromBase64((String)toReturn.get(serializedField), classLoader);
            toReturn.put(serializedField, deserializedValue);
        }

        final List<String> serializedFields = (List<String>)toReturn.remove(KEY_FIELDS_SERIALIZED_TO_STAGING_FILE);
        if (!serializedFields.isEmpty()) {
            try (final ObjectInputStream in = new CustomClassLoadingObjectInputStream(inStream.get(), classLoader)) {
                for (String serializedField : serializedFields) {
                    Object deserializedValue = in.readObject();
                    toReturn.put(serializedField, deserializedValue);
                }
            }
        }
        return toReturn;
    }
}
