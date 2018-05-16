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
 *   Created on Apr 8, 2016 by bjoern
 */
package org.knime.bigdata.spark.local.context;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.util.CustomClassLoadingObjectInputStream;

/**
 * Utility class to handle serialization of job inputs and outputs in local Spark.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class LocalSparkSerializationUtil {

    private static final String KEY_SERIALIZED_FIELDS = "serializedFields";

    /**
     * Serializes all values of the given map into byte arrays, which are not null, boolean, a number or a String.
     * 
     * @param mapToSerialize The map whose values are checked and potentially serialized.
     * @return a new map that contains serialized values in place of the original ones.
     */
    public static Map<String, Object> serializeToPlainJavaTypes(final Map<String, Object> mapToSerialize) {
        final List<String> serializedFields = new LinkedList<>();

        final Map<String, Object> toReturn = new HashMap<>();

        for (Entry<String, Object> entry : mapToSerialize.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();

            if (value == null || value instanceof Boolean || value instanceof Number || value instanceof String) {
            	toReturn.put(key, value);
            } else {
                serializedFields.add(key);
                toReturn.put(key, serialize(value));
            }
        }

        toReturn.put(KEY_SERIALIZED_FIELDS, serializedFields);
        return toReturn;
    }
    
    private static byte[] serialize(final Object object) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
            oos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }
    
    private static Object deserialize(final byte[] bytes, final ClassLoader classLoader) throws ClassNotFoundException, IOException {
        try (final ObjectInputStream ois = new CustomClassLoadingObjectInputStream(new ByteArrayInputStream(bytes), classLoader)) {
            return ois.readObject();
        }
    }

    /**
     * Reverses the effects of the {@link #serializeToPlainJavaTypes(Map)} method.
     * 
     * @param toDeserialize Map with values to deserialize.
     * @param classLoader A classload with which to load the deserialized objects.
     * @return a new map with deserialized objects.
     * @throws ClassNotFoundException f the serialized data contained objects that the given classloader could not
     *             load.
     * @throws IOException if something went wrong during derserialization.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> deserializeFromPlainJavaTypes(Map<String, Object>  toDeserialize, 
    		final ClassLoader classLoader) throws ClassNotFoundException, IOException {
    	
        final Map<String, Object> deserialized = new HashMap<>(toDeserialize);

        final List<String> serializedFields = (List<String>) deserialized.remove(KEY_SERIALIZED_FIELDS);

        for (String serializedField : serializedFields) {
            deserialized.put(serializedField, deserialize((byte[]) deserialized.get(serializedField), classLoader));
        }

        return deserialized;
    }
}
