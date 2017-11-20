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
package org.knime.bigdata.spark.core.jobserver;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.knime.bigdata.spark.core.job.SparkClass;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;

/**
 * @author Bjoern Lohrmann, KNIME.COM
 */
@SparkClass
public class TypesafeConfigSerializationUtils {

    private static final String KEY_SERIALIZED_FIELDS = "serializedFields";

    /**
     * Serializes the givenmap into a Typesafe {@link Config} object. Type restrictions of Typesafe's {@link Config} are
     * worked around when necessary by Base64 serialization.
     *
     * @param mapToSerialize a map to serialize
     * @return the HOCON data as a string
     */
    public static Config serializeToTypesafeConfig(final Map<String, Object> mapToSerialize) {
        final List<String> serializedFields = new LinkedList<>();

        Config typesafeConfig = ConfigFactory.empty();

        for (Entry<String, Object> entry : mapToSerialize.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();

            // the typesafe library can only handle the JSON values, but even may change JSON-supported values such as lists
            // and maps. Therefore we serialize anything to base64 that is not null, a String or a boxed primitive type.
            if (value == null || value instanceof Boolean || value instanceof Number || value instanceof String) {
                typesafeConfig = typesafeConfig.withValue(key, ConfigValueFactory.fromAnyRef(value));
            } else {
                serializedFields.add(key);
                typesafeConfig = typesafeConfig.withValue(key,
                    ConfigValueFactory.fromAnyRef(Base64SerializationUtils.serializeToBase64(value)));
            }
        }

        // serialized fields is guaranteed to be still free (by convention)
        typesafeConfig = typesafeConfig.withValue(KEY_SERIALIZED_FIELDS, ConfigValueFactory.fromAnyRef(serializedFields));

        return typesafeConfig;
    }

    /**
     * Deserializes the Typesafe {@link Config} object into a map.
     *
     * @param configToDeserialize The Typesafe {@link Config} to deserialize
     * @param classLoader Class loader to use during deserialization
     * @return map a map with key-value pairs
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> deserializeFromTypesafeConfig(final Config configToDeserialize, final ClassLoader classLoader) throws ClassNotFoundException, IOException {
        final Map<String, Object> deserialized = new HashMap<>();

        for(Map.Entry<String, ConfigValue> entry : configToDeserialize.entrySet()) {
            deserialized.put(entry.getKey(), entry.getValue().unwrapped());
        }

        final List<String> serializedFields = (List<String>) deserialized.remove(KEY_SERIALIZED_FIELDS);

        for (String serializedField : serializedFields) {
            deserialized.put(serializedField, Base64SerializationUtils.deserializeFromBase64((String) deserialized.get(serializedField), classLoader));
        }

        return deserialized;
    }
}
