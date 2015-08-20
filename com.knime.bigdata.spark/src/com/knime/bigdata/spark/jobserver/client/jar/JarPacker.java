package com.knime.bigdata.spark.jobserver.client.jar;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.knime.core.node.KNIMEConstants;

/**
 * add the byte code of the given class to a copy of an existing jar file (put together from a number of different
 * sources)
 */
public class JarPacker {

    /**
     * add the given byte code to the given jar and put it into a new jar
     *
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
        try(final JarFile source = new JarFile(aSourceJarPath);
                final FileOutputStream fos = new FileOutputStream(aTargetJarPath);
                final JarOutputStream target = new JarOutputStream(fos);) {
            final String packagePath;
            if (aPackagePath.length() > 0) {
                packagePath = aPackagePath.replaceAll("\\.", "/") + "/";
            } else {
                packagePath = "";
            }
            copyJarFile(source, target);
            for (Map.Entry<String, byte[]> entry : aClassByteCodes.entrySet()) {
                final String classPath = packagePath + entry.getKey() + ".class";
                addClass(classPath, entry.getValue(), target);
            }
        }
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

        final File f = new File(aSourceJarPath);
        if (!f.exists()) {
            throw new IOException("Error: input jar file " + f.getAbsolutePath() + " does not exist!");
        }
        final JarFile source = new JarFile(aSourceJarPath);
        try (final JarOutputStream target = new JarOutputStream(new FileOutputStream(aTargetJarPath))) {
            copyJarFile(source, target);

            final String path = aClassPath.replaceAll("\\.", "/");
            final List<String> classPath = new ArrayList<>();
            classPath.add(path);
            final Class<?> c = Class.forName(aClassPath);

            Class<?>[] c2 = c.getDeclaredClasses();
            for (Class<?> innerClass : c2) {
                classPath.add(path + "$" + innerClass.getSimpleName());
            }

            for (String cp : classPath) {
                final String prefix = "/" + cp;
                {
                    final InputStream is = c.getResourceAsStream(prefix + ".class");
                    if (is != null) {
                        copyEntry(cp + ".class", is, target);
                        is.close();
                    }
                }
                int ix = 1;
                do {
                    //now try anonymous inner classes with '$ix.class'
                    final String name = "$" + ix + ".class";
                    final InputStream is = c.getResourceAsStream(prefix + name);
                    if (is == null) {
                        break;
                    }
                    copyEntry(cp + name, is, target);
                    is.close();
                    ix++;
                } while (true);
            }
        }
        source.close();
    }

    /**
     * @param jarFile the File to write to
     * @param aPackagePath
     * @param aByteCode the bytecode map
     * @throws IOException if the jar could not be created
     */
    public static void createJar(final File jarFile, final String aPackagePath, final Map<String, byte[]> aByteCode)
            throws IOException {
        final String packagePath;
        if (aPackagePath.length() > 0) {
            packagePath = aPackagePath.replaceAll("\\.", "/") + "/";
        } else {
            packagePath = "";
        }
        try (final OutputStream os = Files.newOutputStream(jarFile.toPath(), StandardOpenOption.CREATE);
                final JarOutputStream jos = new JarOutputStream(os, createManifest())) {
            for (String className : aByteCode.keySet()) {
                final String classPath = packagePath + className + ".class";
                addClass(classPath, aByteCode.get(className), jos);
            }
        }
    }

    private static Manifest createManifest() {
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VENDOR, "KNIME.com");
        manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VERSION, KNIMEConstants.VERSION);
        return manifest;
    }

    /**
     * @param source1 the path to first jar file to merge
     * @param source2 the path to second jar file to merge
     * @param target the File that should contain the content of both source jars
     * @throws IOException if the jar file can not be created
     */
    public static void mergeJars(final String source1, final String source2, final File target) throws IOException {
        final Set<String> filterEntries = new HashSet<String>(1);
        filterEntries.add("META-INF/MANIFEST.MF");
        try (final OutputStream os = Files.newOutputStream(target.toPath(), StandardOpenOption.CREATE);
                final JarOutputStream jos = new JarOutputStream(os, createManifest())) {
            try (final JarFile s1 = new JarFile(source1);
                    final JarFile s2 = new JarFile(source2);) {
                copyJarFile(s1, jos, filterEntries);
                copyJarFile(s2, jos, filterEntries);
            }
        }
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
        copyJarFile(aSourceJarFile, aTargetOutputStream, Collections.<String> emptySet());
    }

    private static void copyJarFile(final JarFile aSourceJarFile, final JarOutputStream aTargetOutputStream,
        final Set<String> entryNames2Filter) throws IOException {
        Enumeration<JarEntry> entries = aSourceJarFile.entries();

        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            try(InputStream is = aSourceJarFile.getInputStream(entry);){
                final String entryName = entry.getName();
                if (entryNames2Filter == null || !entryNames2Filter.contains(entryName)) {
                    copyEntry(entryName, is, aTargetOutputStream);
                }
            }
            aTargetOutputStream.flush();
            aTargetOutputStream.closeEntry();
        }
    }

    /**
     * This method copies all entries except the filter entries from the given jar file into a new temp file which in
     * the end replaces the input file.
     *
     * @param jarFile the jar file to remove the given classes from
     * @param entryNames the names of the jar entries to remove
     * @throws IOException if a new file could not be created
     */
    public static void removeFromJar(final File jarFile, final Set<String> entryNames) throws IOException {
        final File tempFile = File.createTempFile("snippet", ".jar", jarFile.getParentFile());
        final String jarFilePath = jarFile.getPath();
        try (final JarFile source = new JarFile(jarFilePath);
                final JarOutputStream target = new JarOutputStream(new FileOutputStream(tempFile));) {
            copyJarFile(source, target, entryNames);
        }
        Files.move(tempFile.toPath(), jarFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }
}
