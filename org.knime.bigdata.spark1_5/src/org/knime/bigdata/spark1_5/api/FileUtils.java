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
 *   Created on May 27, 2019 by bjoern
 */
package org.knime.bigdata.spark1_5.api;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.spark.SparkContext;
import org.apache.spark.util.Utils;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.util.TempFileSupplier;

/**
 * Spark-side utility class to create temporary files and folders, as well easy zip file operations.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @since 2.5.0
 */
@SparkClass
public class FileUtils implements TempFileSupplier {

    private static FileUtils SINGLETON_INSTANCE;

    private final SparkContext m_sparkContext;

    /**
     * Non public constructor for singleton instance.
     *
     * @param sparkContext
     */
    protected FileUtils(final SparkContext sparkContext) {
        m_sparkContext = sparkContext;
    }

    /**
     *
     * @param sparkContext A Spark context to figure out Spark's scratch directory.
     * @return a {@link TempFileSupplier} instance that creates temporary files in Spark's scratch directory.
     */
    public synchronized static TempFileSupplier getTempFileSupplier(final SparkContext sparkContext) {
        if (SINGLETON_INSTANCE == null) {
            SINGLETON_INSTANCE = new FileUtils(sparkContext);
        }

        return SINGLETON_INSTANCE;
    }

    /**
     * Creates a new temporary directory.
     *
     * @param sparkContext A Spark context to figure out Spark's scratch directory.
     * @param prefix Desired prefix of the directory name. May be null.
     * @return a newly created temporary directory.
     * @throws IOException
     */
    public static Path createTempDir(final SparkContext sparkContext, final String prefix) throws IOException {
        for (final String localDir : Utils.getConfiguredLocalDirs(sparkContext.conf())) {
            return Files.createTempDirectory(Paths.get(localDir), prefix);
        }
        // never happens
        return null;
    }

    /**
     * Creates a new temporary file.
     *
     * @param sparkContext A Spark context to figure out Spark's scratch directory.
     * @param prefix Desired prefix of the file name. May be null.
     * @param suffix Desired suffix of the file name. May be null.
     * @return a newly created temporary file.
     * @throws IOException
     */
    public static Path createTempFile(final SparkContext sparkContext, final String prefix, final String suffix)
        throws IOException {
        for (final String localDir : Utils.getConfiguredLocalDirs(sparkContext.conf())) {
            return Files.createTempFile(Paths.get(localDir), prefix, suffix);
        }
        // never happens
        return null;
    }

    /**
     * Zips the given directory and writes the zipped data to the given file.
     *
     * @param sourceDirPath The directory to zip.
     * @param zipFilePath The file to write the zipped data to.
     * @throws IOException
     */
    public static void zipDirectory(final Path sourceDirPath, final Path zipFilePath) throws IOException {
        try (final ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(zipFilePath))) {
            Files.walkFileTree(sourceDirPath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                    ZipEntry zipEntry = new ZipEntry(sourceDirPath.relativize(file).toString());
                    try {
                        zs.putNextEntry(zipEntry);
                        Files.copy(file, zs);
                        zs.closeEntry();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(final Path dir, final IOException e) throws IOException {
                    if (e == null) {
                        return FileVisitResult.CONTINUE;
                    } else {
                        // directory iteration failed
                        throw e;
                    }
                }
            });
        }
    }

    /**
     * Unzips the given zip file to the given destination directory.
     *
     * @param zipFilePath The file to unzip.
     * @param destDir The directory to unzip to.
     * @throws IOException
     */
    public static void unzipToDirectory(final Path zipFilePath, final Path destDir) throws IOException {
        if (Files.notExists(destDir)) {
            throw new IOException("Destination directory does not exist: " + destDir);
        }
        if (!Files.isDirectory(destDir)) {
            throw new IOException("Destination is not a directory: " + destDir);
        }
        ZipInputStream in = new ZipInputStream(Files.newInputStream(zipFilePath));
        unzip(in, destDir, 0);
    }

    /**
     * Stores the content of the zip stream in the specified directory. If a strip level larger than zero is specified,
     * it strips off that many path segments from the zip entries. If the zip stream contains elements with less path
     * segments, they all end up directly in the specified dir.
     *
     * @param zipStream must contain a zip archive. Is unpacked an stored in the specified directory.
     * @param destDir the destination directory the content of the zip stream is stored in
     * @param stripLevel the number of path segments (directory levels) striped off the file (and dir) names in the zip
     *            archive.
     * @throws IOException if it was not able to store the content
     */
    public static void unzip(final ZipInputStream zipStream, final Path destDir, final int stripLevel)
        throws IOException {

        final Path normalizedDestDir = destDir.normalize();

        ZipEntry e;
        while ((e = zipStream.getNextEntry()) != null) {

            String name = e.getName().replace('\\', '/');
            name = stripOff(name, stripLevel);

            final Path newFile = normalizedDestDir.resolve(name).normalize();
            if (!newFile.startsWith(normalizedDestDir)) {
                throw new IOException("Refusing to create files outside of given destination directory");
            }

            if (e.isDirectory()) {
                if (!name.isEmpty() && !name.equals("/")) {
                    Files.createDirectories(newFile);
                }
            } else {
                Files.createDirectories(newFile.getParent());
                Files.copy(zipStream, newFile);
            }
        }
        zipStream.close();
    }

    /**
     * Recursively deletes the given folder or file.
     *
     * @param toDelete Folder or file to delete.
     * @throws IOException
     */
    public static void deleteRecursively(final Path toDelete) throws IOException {
        Files.walkFileTree(toDelete, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException e) throws IOException {
                if (e == null) {
                    Files.deleteIfExists(dir);
                    return FileVisitResult.CONTINUE;
                } else {
                    // directory iteration failed
                    throw e;
                }
            }
        });
    }

    /**
     * Strip off the path the specified amount of segments. Segment separator must be a '/'.
     *
     * @param path the string from which the first <code>level</code> segments are stripped off
     * @param level the number of segments that are stripped off.
     * @return the specified <code>path</code> with the first <code>level</code> segments (that is directories) stripped
     *         off.
     */
    private static String stripOff(final String path, final int level) {
        if (path == null) {
            return null;
        }
        int l = level;
        if (!path.isEmpty() && path.charAt(0) == '/') {
            l++;
        }
        String[] segm = path.split("/", l + 1);
        return segm[segm.length - 1];
    }

    /**
     * {@inheritDoc}
     *
     * @throws IOException
     */
    @Override
    public Path newTempFile() throws IOException {
        return createTempFile(m_sparkContext, null, null);
    }
}
