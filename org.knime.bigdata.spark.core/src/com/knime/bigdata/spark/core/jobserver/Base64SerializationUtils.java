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
 *   Created on Apr 6, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.jobserver;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.xml.bind.DatatypeConverter;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Bjoern Lohrmann, KNIME.COM
 */
@SparkClass
public class Base64SerializationUtils {

    /**
     * Serializes the given object to a Base64 string.
     *
     * @param object object to be serialized
     * @return string representation of given object
     *
     */
    public static String serializeToBase64(final Object object) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
            oos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return DatatypeConverter.printBase64Binary(baos.toByteArray());
    }

    /**
     * Serializes the given object to a file in the local file system.
     *
     * @param object The object to serialize
     * @param file The file to serialize to (if it exists, contents will be overwritten)
     */
    public static void serializeToFile(final Object object, final File file) {
        try (final ObjectOutputStream oos =
            new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
            oos.writeObject(object);
            oos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deserialize an object from a file and cast it to the given type.
     *
     * @param file File in the local file system to read from
     * @return the deserialized object
     *
     * @throws KNIMESparkException if the file cannot be read, T cannot be found or the data is corrupted
     */
    @SuppressWarnings("unchecked")
    protected static <T> T deserializeFromFile(final File file) throws KNIMESparkException {
        try (final ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            return (T)ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new KNIMESparkException(e);
        }
    }

    /**
     * Deserializes the given Base64 String into an object.
     *
     * @param base64String the Base64 String to decode
     * @param classLoader class loader to use
     * @return the decoded Object
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserializeFromBase64(final String base64String, final ClassLoader classLoader) throws IOException, ClassNotFoundException {
        byte[] data = DatatypeConverter.parseBase64Binary(base64String);
        try (final ObjectInputStream ois = new CustomClassLoadingObjectInputStream(new ByteArrayInputStream(data), classLoader)) {
            return (T) ois.readObject();
        }
    }
}
