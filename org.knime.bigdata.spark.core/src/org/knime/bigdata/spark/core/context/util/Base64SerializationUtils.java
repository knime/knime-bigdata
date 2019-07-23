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
package org.knime.bigdata.spark.core.context.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.xml.bind.DatatypeConverter;

import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.util.CustomClassLoadingObjectInputStream;

/**
 * Utility class that (de)serializes objects from/to base64 encoded Strings.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
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
     * Serializes the contents of the given file to a gzip-compressed Base64 string.
     *
     * @param file file that is supposed to be serialized to a Base64 string.
     * @return gzip-compressed Base64 representation of given file contents.
     *
     */
    public static String serializeToBase64Compressed(final Path file) {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream out = new GZIPOutputStream(baos)) {
            Files.copy(file, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return DatatypeConverter.printBase64Binary(baos.toByteArray());
    }

    /**
     * Deserializes the given gzip-compressed Base64 string into a file.
     *
     * @param base64String Gzip-compressed Base64 representation of desired file content.
     * @param destFile File to write to.
     */
    public static void deserializeFromBase64Compressed(final String base64String, final Path destFile) {
        try (InputStream in =
            new GZIPInputStream(new ByteArrayInputStream(DatatypeConverter.parseBase64Binary(base64String)))) {
            Files.copy(in, destFile, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
