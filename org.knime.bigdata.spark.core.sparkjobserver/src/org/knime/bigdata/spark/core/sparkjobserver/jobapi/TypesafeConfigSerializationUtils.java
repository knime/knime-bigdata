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
package org.knime.bigdata.spark.core.sparkjobserver.jobapi;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.knime.bigdata.spark.core.context.util.Base64SerializationUtils;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.WrapperJobOutput;
import org.knime.bigdata.spark.core.util.TempFileSupplier;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;

/**
 * Utility class that helps serialize data from/to Typesafe's {@link Config} objects, which are supported by Spark
 * Jobserver as input and return values for jobs.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class TypesafeConfigSerializationUtils {

    private static final String KEY_SERIALIZED_FIELDS = "serializedFields";

    private static final String KEY_FILES = "files";

    private static final String KEY_FILE_CONTENTS = "fileContents";

    /**
     * Serializes the given {@link JobserverJobInput} into a Typesafe {@link Config} object. Type restrictions of
     * Typesafe's {@link Config} are worked around when necessary by Base64 serialization. The paths of the files
     * attached to the given input will part of the resulting HOCON data (not the file contents).
     * 
     *
     * @param jsInput input to serialize.
     * @return the HOCON data as a string
     */
    public static Config serializeToTypesafeConfig(final JobserverJobInput jsInput) {
        final List<String> serializedFields = new LinkedList<>();

        Config typesafeConfig = ConfigFactory.empty();

        for (Entry<String, Object> entry : jsInput.getInternalMap().entrySet()) {
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
        typesafeConfig =
            typesafeConfig.withValue(KEY_SERIALIZED_FIELDS, ConfigValueFactory.fromAnyRef(serializedFields));

        final List<String> files = new LinkedList<>();
        for (Path file : jsInput.getFiles()) {
            files.add(file.toString());
        }
        typesafeConfig = typesafeConfig.withValue(KEY_FILES, ConfigValueFactory.fromIterable(files));

        return typesafeConfig;
    }

    /**
     * Serializes the given {@link WrapperJobOutput} into a Typesafe {@link Config} object. Type restrictions of
     * Typesafe's {@link Config} are worked around when necessary by Base64 serialization. The contents of the files
     * attached to the given {@link WrapperJobOutput} will part of the resulting HOCON data.
     * 
     *
     * @param wrapperJobOutput output to serialize.
     * @return the HOCON data as a string
     */
    public static Config serializeToTypesafeConfig(final WrapperJobOutput wrapperJobOutput) {
        final List<String> serializedFields = new LinkedList<>();

        Config typesafeConfig = ConfigFactory.empty();

        for (Entry<String, Object> entry : wrapperJobOutput.getInternalMap().entrySet()) {
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
        typesafeConfig =
            typesafeConfig.withValue(KEY_SERIALIZED_FIELDS, ConfigValueFactory.fromAnyRef(serializedFields));

        final List<String> fileContents = new LinkedList<>();
        for (Path file : wrapperJobOutput.getFiles()) {
            fileContents.add(Base64SerializationUtils.serializeToBase64Compressed(file));
        }
        typesafeConfig = typesafeConfig.withValue(KEY_FILE_CONTENTS, ConfigValueFactory.fromIterable(fileContents));

        return typesafeConfig;
    }

    /**
     * Deserializes the Typesafe {@link Config} object into a map.
     *
     * @param configToDeserialize The Typesafe {@link Config} to deserialize
     * @return map a map with key-value pairs
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("unchecked")
    public static JobserverJobInput deserializeJobserverJobInput(final Config configToDeserialize)
        throws ClassNotFoundException, IOException {

        final JobserverJobInput toReturn = new JobserverJobInput();
        final Map<String, Object> deserializedInternalMap = toReturn.getInternalMap();

        for (Map.Entry<String, ConfigValue> entry : configToDeserialize.entrySet()) {
            deserializedInternalMap.put(entry.getKey(), entry.getValue().unwrapped());
        }

        final List<String> serializedFields = (List<String>)deserializedInternalMap.remove(KEY_SERIALIZED_FIELDS);
        for (String serializedField : serializedFields) {
            deserializedInternalMap.put(serializedField, Base64SerializationUtils.deserializeFromBase64(
                (String)deserializedInternalMap.get(serializedField), toReturn.getClass().getClassLoader()));
        }

        final List<String> files = (List<String>)deserializedInternalMap.remove(KEY_FILES);
        for (String file : files) {
            toReturn.withFile(Paths.get(file));
        }
        return toReturn;
    }

    /**
     * Deserializes a {@link WrapperJobOutput} object from the given Typesafe {@link Config}. This method is the
     * counterpart to {@link #serializeToTypesafeConfig(WrapperJobOutput)}.
     * 
     * @param configToDeserialize Typesafe {@link Config} to deserialize.
     * @param classLoader Classloader with which to deserialize complex fields.
     * @param tmpFileSupplier Used to create temporary files that were attached during
     *            {@link #serializeToTypesafeConfig(WrapperJobOutput)}.
     * @return A newly created {@link WrapperJobOutput} object.
     * @throws ClassNotFoundException
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static WrapperJobOutput deserializeWrapperJobOutput(final Config configToDeserialize,
        final ClassLoader classLoader, final TempFileSupplier tmpFileSupplier)
        throws ClassNotFoundException, IOException {

        final WrapperJobOutput toReturn = new WrapperJobOutput();
        final Map<String, Object> deserialized = toReturn.getInternalMap();

        for (Map.Entry<String, ConfigValue> entry : configToDeserialize.entrySet()) {
            deserialized.put(entry.getKey(), entry.getValue().unwrapped());
        }

        final List<String> serializedFields = (List<String>)deserialized.remove(KEY_SERIALIZED_FIELDS);

        for (String serializedField : serializedFields) {
            deserialized.put(serializedField,
                Base64SerializationUtils.deserializeFromBase64((String)deserialized.get(serializedField), classLoader));
        }

        final List<String> fileContents = (List<String>)deserialized.remove(KEY_FILE_CONTENTS);
        for (String fileContent : fileContents) {
            final Path tmpFile = tmpFileSupplier.newTempFile();
            Base64SerializationUtils.deserializeFromBase64Compressed(fileContent, tmpFile);
            toReturn.withFile(tmpFile);
        }

        return toReturn;
    }
}
