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
 *   Created on 22.09.2015 by koetter
 */
package com.knime.bigdata.spark.core.jar;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

import org.apache.commons.codec.binary.Hex;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.Version;

import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * This is a file based jar collector.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class FileBasedJarCollector implements JarCollector {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(FileBasedJarCollector.class);

    private final SparkVersion m_sparkVersion;

    private final File m_jarFile;

    private final OutputStream m_os;

    private final JarOutputStream m_jos;

    private final MessageDigest m_digest;

    private JobJar m_jobJar;

    private String m_jobserverJobClass;

    /**
     * @param sparkVersion the Spark version
     */
    public FileBasedJarCollector(final SparkVersion sparkVersion) {
        m_sparkVersion = sparkVersion;
        try {
            m_jarFile = File.createTempFile("sparkClasses",  ".jar");
            m_jarFile.deleteOnExit();
            LOGGER.debug("Creating Spark job jar file: " + m_jarFile.getAbsolutePath());
            m_os = new BufferedOutputStream(Files.newOutputStream(m_jarFile.toPath(), StandardOpenOption.CREATE));
            m_jos = new JarOutputStream(m_os, JarPacker.createManifest());
        } catch (IOException e) {
            LOGGER.warn("Error creating spark jar collector: " + e.getMessage());
            throw new RuntimeException(e);
        }

        try {
            m_digest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Could not create SHA-1 hash of job jar", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobJar getJobJar() {
        ensureFinalized();
        return m_jobJar;
    }

    /**
     * @throws IOException
     */
    private void ensureFinalized() {
        if (m_jobJar != null) {
            return;
        }

        try {
            final JobJarDescriptor descriptor = createDescriptor();
            m_jos.putNextEntry(new JarEntry(JobJarDescriptor.FILE_NAME));
            descriptor.save(m_jos);

            m_jos.flush();
            m_jos.close();
            m_os.flush();
            m_os.close();

            m_jobJar = new JobJar(m_jarFile, descriptor);
        } catch (IOException e) {
            LOGGER.warn("Exception closing jar output stream: " + e.getMessage());
        }
    }

    private JobJarDescriptor createDescriptor() throws IOException, FileNotFoundException {
        m_jos.flush();
        m_os.flush();

        final String sha1Hex = Hex.encodeHexString(m_digest.digest());
        final Version sparkCorePluginVersion = FrameworkUtil.getBundle(JarPacker.class).getVersion();

        final JobJarDescriptor jarInfo = new JobJarDescriptor(sparkCorePluginVersion.toString(),
            m_sparkVersion.toString(), sha1Hex, m_jobserverJobClass);

        return jarInfo;
    }

    private void addToDigest(final String stringData) {
        try {
            m_digest.update(stringData.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e1) {
            // do nothing, won't happen
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addJar(final File jar) {
        addJar(jar, Collections.singleton(JarPacker.MANIFEST_MF_FILTER));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addJar(final File jar, final Set<Predicate<JarEntry>> filterPredicates) {
        if (m_jobJar != null) {
            throw new IllegalStateException("Job jar has already been finalized");
        }

        addToDigest(jar.getName());

        try (final JarFile jarFile = new JarFile(jar)) {
            JarPacker.copyJarFile(jarFile, m_jos, filterPredicates);
        } catch (IOException e) {
            LOGGER.warn("Exception adding jar " + jar.getPath() + " Exception: " + e.getMessage());
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void addDirectory(final File dir) {
        if (m_jobJar != null) {
            throw new IllegalStateException("Job jar has already been finalized");
        }

        final String[] fileNames = dir.list();
        for (final String fileName : fileNames) {
            final File file = new File(dir, fileName);
            if (file.isDirectory()) {
                //only look at the bin directories
                addDirectory(file);
            } else {
                addFile(file.getPath(), file);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addFile(final String entryName, final File file) {
        if (m_jobJar != null) {
            throw new IllegalStateException("Job jar has already been finalized");
        }

        addToDigest(entryName);

        try (final BufferedInputStream is = new BufferedInputStream(new FileInputStream(file))) {
            JarPacker.copyEntry(entryName, is, m_jos);
        } catch (Exception e) {
            LOGGER.warn("Exception adding file " + file + " to jar: " + e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addJarEntry(final JarEntry je, final InputStream is) {
        if (m_jobJar != null) {
            throw new IllegalStateException("Job jar has already been finalized");
        }

        addToDigest(je.getName());
        try {
            JarPacker.copyEntry(je.getName(), is, m_jos);
        } catch (IOException e) {
            LOGGER.warn("Exception jar entry " + je.getName() + " to jar: " + e.getMessage());
        }
    }

    private byte[] readIntoByteArray(final InputStream is) throws IOException {
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        byte[] buf = new byte[16 * 1024];

        int read = 0;
        while((read = is.read(buf)) != -1) {
            byteArray.write(buf, 0, read);
        }

        return byteArray.toByteArray();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setJobserverJobClass(final String jobserverJobClass) {
        m_jobserverJobClass = jobserverJobClass;
    }
}
