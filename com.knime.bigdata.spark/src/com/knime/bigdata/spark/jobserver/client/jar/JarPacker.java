package com.knime.bigdata.spark.jobserver.client.jar;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

/**
 * add the byte code of the given class to a copy of an existing jar file (put together from a number of different
 * sources) TODO - add original authors
 */
public class JarPacker {

    static void add2Jar(final String aSourceJarPath, final String aTargetJarPath, final String aClassPath,
        final byte[] aByteCode) throws IOException {

        final File f = new File(aSourceJarPath);
        if (!f.exists()) {
            throw new IOException("Error: input jar file " + f.getAbsolutePath() + " does not exist!");
        }
        final JarFile source = new JarFile(aSourceJarPath);

        try (final JarOutputStream target = new JarOutputStream(new FileOutputStream(aTargetJarPath))) {
            copyJarFile(source, target);
            final String classPath = aClassPath.replaceAll("\\.", "/") + ".class";
            addClass(classPath, aByteCode, target);
        }
    }

    static void add2Jar(final String aSourceJarPath, final String aTargetJarPath, final String aClassPath) throws IOException, ClassNotFoundException {

        final String classPath = aClassPath.replaceAll("\\.", "/") + ".class";

        final File f = new File(aSourceJarPath);
        if (!f.exists()) {
            throw new IOException("Error: input jar file " + f.getAbsolutePath() + " does not exist!");
        }
        Class<?> c = Class.forName(aClassPath);
        InputStream is = c.getResourceAsStream("/"+classPath);

        final JarFile source = new JarFile(aSourceJarPath);
        try (final JarOutputStream target = new JarOutputStream(new FileOutputStream(aTargetJarPath))) {
            copyJarFile(source, target);
            copyEntry(classPath, is, target);
        }
    }

    private static void addClass(final String aClassPath, final byte[] aByteCode, final JarOutputStream aTargetOutputStream)
        throws IOException {
        final JarEntry entry = new JarEntry(aClassPath);
        entry.setTime(System.currentTimeMillis());
        aTargetOutputStream.putNextEntry(entry);
        aTargetOutputStream.write(aByteCode);
        aTargetOutputStream.closeEntry();
    }

    private static void copyEntry(final String aClassPath, final InputStream is, final JarOutputStream aTargetOutputStream)
        throws IOException {
        final JarEntry copy = new JarEntry(aClassPath);
        // create a new entry to avoid ZipException: invalid entry
        // compressed size
        aTargetOutputStream.putNextEntry(copy);
        byte[] buffer = new byte[4096];
        int bytesRead = 0;
        while ((bytesRead = is.read(buffer)) != -1) {
            aTargetOutputStream.write(buffer, 0, bytesRead);
        }
    }

    private static void copyJarFile(final JarFile aSourceJarFile, final JarOutputStream aTargetOutputStream)
        throws IOException {
        Enumeration<JarEntry> entries = aSourceJarFile.entries();

        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            InputStream is = aSourceJarFile.getInputStream(entry);
            copyEntry(entry.getName(), is, aTargetOutputStream);
            is.close();
            aTargetOutputStream.flush();
            aTargetOutputStream.closeEntry();
        }
    }


}
