package com.knime.bigdata.spark.jobserver.client.jar;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

/**
 * add the byte code of the given class to a copy of an existing jar file (put together from a number of different
 * sources) TODO - add original authors
 */
public class JarPacker {

    /**
     * add the given byte code to the given jar and put it into a new jar
     * @param aSourceJarPath
     * @param aTargetJarPath
     * @param aPackagePath
     * @param aClassByteCodes
     * @throws IOException
     */
    public static void add2Jar(final String aSourceJarPath, final String aTargetJarPath, final String aPackagePath,
        final Map<String, byte[]> aClassByteCodes) throws IOException {

        final File f = new File(aSourceJarPath);
        if (!f.exists()) {
            throw new IOException("Error: input jar file " + f.getAbsolutePath() + " does not exist!");
        }
        final JarFile source = new JarFile(aSourceJarPath);

        final String packagePath;
        if (aPackagePath.length() > 0) {
            packagePath = aPackagePath.replaceAll("\\.", "/") + "/";
        } else {
            packagePath = "";
        }
        try (final JarOutputStream target = new JarOutputStream(new FileOutputStream(aTargetJarPath))) {
            copyJarFile(source, target);
            for (Map.Entry<String, byte[]> entry : aClassByteCodes.entrySet()) {
                final String classPath = packagePath + entry.getKey() + ".class";
                addClass(classPath, entry.getValue(), target);
            }
        }

        source.close();
    }

    /**
     *
     * @param aSourceJarPath
     * @param aTargetJarPath
     * @param aClassPath
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static void add2Jar(final String aSourceJarPath, final String aTargetJarPath, final String aClassPath)
        throws IOException, ClassNotFoundException {

        final String path = aClassPath.replaceAll("\\.", "/");
        //TODO - is there a better way to determine inner classes?
        final List<String> classPath = new ArrayList<>();
        classPath.add(path + ".class");
        classPath.add(path + "$1.class");
        classPath.add(path + "$2.class");
        classPath.add(path + "$3.class");
        classPath.add(path + "$4.class");
        final Class<?> c = Class.forName(aClassPath);
        Class<?>[] c2 = c.getDeclaredClasses();
        for (Class<?> innerClass : c2) {
            classPath.add(path+"$"+innerClass.getSimpleName()+".class");
            classPath.add(path+"$"+innerClass.getSimpleName()+"$1.class");
            classPath.add(path+"$"+innerClass.getSimpleName()+"$2.class");
            classPath.add(path+"$"+innerClass.getSimpleName()+"$3.class");
            classPath.add(path+"$"+innerClass.getSimpleName()+"$4.class");
        }
        final File f = new File(aSourceJarPath);
        if (!f.exists()) {
            throw new IOException("Error: input jar file " + f.getAbsolutePath() + " does not exist!");
        }
        final JarFile source = new JarFile(aSourceJarPath);
        try (final JarOutputStream target = new JarOutputStream(new FileOutputStream(aTargetJarPath))) {
            copyJarFile(source, target);

            for (String cp : classPath) {
                final InputStream is = c.getResourceAsStream("/" + cp);
                if (is != null) {
                    copyEntry(cp, is, target);
                    is.close();
                }
            }
        }
        source.close();
    }

    private static void addClass(final String aClassPath, final byte[] aByteCode,
        final JarOutputStream aTargetOutputStream) throws IOException {
        final JarEntry entry = new JarEntry(aClassPath);
        entry.setTime(System.currentTimeMillis());
        aTargetOutputStream.putNextEntry(entry);
        aTargetOutputStream.write(aByteCode);
        aTargetOutputStream.closeEntry();
    }

    private static void copyEntry(final String aClassPath, final InputStream is,
        final JarOutputStream aTargetOutputStream) throws IOException {
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
        copyJarFile(aSourceJarFile, aTargetOutputStream, Collections.<String>emptySet());
    }

    private static void copyJarFile(final JarFile aSourceJarFile, final JarOutputStream aTargetOutputStream,
        final Set<String> entryNames2Filter)
        throws IOException {
        Enumeration<JarEntry> entries = aSourceJarFile.entries();

        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            InputStream is = aSourceJarFile.getInputStream(entry);
            final String entryName = entry.getName();
            if (entryNames2Filter == null || !entryNames2Filter.contains(entryName)) {
                copyEntry(entryName, is, aTargetOutputStream);
            }
            is.close();
            aTargetOutputStream.flush();
            aTargetOutputStream.closeEntry();
        }
    }

    /**
     * This method copies all entries except the filter entries from the given jar file into a new temp file
     * which in the end replaces the input file.
     * @param jarFile the jar file to remove the given classes from
     * @param entryNames the names of the jar entries to remove
     * @throws IOException if a new file could not be created
     */
    public static void removeFromJar(final File jarFile, final Set<String> entryNames) throws IOException {
        final File tempFile = File.createTempFile("snippet", ".jar", jarFile.getParentFile());
        final String jarFilePath = jarFile.getPath();
        try (   final JarFile source = new JarFile(jarFilePath);
                final JarOutputStream target = new JarOutputStream(new FileOutputStream(tempFile));) {
            copyJarFile(source, target, entryNames);
        }
        jarFile.delete();
        tempFile.renameTo(jarFile);
    }
}
