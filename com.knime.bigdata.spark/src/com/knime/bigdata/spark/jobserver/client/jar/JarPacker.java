package com.knime.bigdata.spark.jobserver.client.jar;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

/**
 * add the byte code of the given class to a copy of an existing jar file
 * (put together from a number of different sources)
 * TODO - add original authors
 */
public class JarPacker {

    static void add2Jar(final String aSourceJarPath, final String aTargetJarPath, final String aClassPath,
        final byte[] aByteCode) throws IOException {

        final JarFile source = new JarFile(aSourceJarPath);

        final JarOutputStream target = new JarOutputStream(new FileOutputStream(aTargetJarPath));
        copyJarFile(source, target);
        addClass(aClassPath, aByteCode, target);
        target.close();
    }

    private static void addClass(final String aClassPath, final byte[] aByteCode, final JarOutputStream target)
        throws IOException {
        final JarEntry entry = new JarEntry(aClassPath.replaceAll("\\.", "/") + ".class");
        entry.setTime(System.currentTimeMillis());
        target.putNextEntry(entry);
        target.write(aByteCode);
        target.closeEntry();
    }

    private static void copyJarFile(final JarFile aSourceJarFile, final JarOutputStream aTargetOutputStream) throws IOException {
        Enumeration<JarEntry> entries = aSourceJarFile.entries();

        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            InputStream is = aSourceJarFile.getInputStream(entry);

            // jos.putNextEntry(entry);
            // create a new entry to avoid ZipException: invalid entry
            // compressed size
            aTargetOutputStream.putNextEntry(new JarEntry(entry.getName()));
            byte[] buffer = new byte[4096];
            int bytesRead = 0;
            while ((bytesRead = is.read(buffer)) != -1) {
                aTargetOutputStream.write(buffer, 0, bytesRead);
            }
            is.close();
            aTargetOutputStream.flush();
            aTargetOutputStream.closeEntry();
        }
    }

}
