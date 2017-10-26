package com.knime.tpbuilder.osgi

import java.io.File
import aQute.lib.osgi.Jar

import collection.JavaConverters._
import aQute.lib.osgi.Resource
import java.util.regex.Pattern
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter
import java.io.InputStreamReader
import java.io.BufferedReader
import scala.collection.immutable.Stream
import scala.io.Source
import scala.io.Codec
import java.io.BufferedWriter
import aQute.lib.osgi.EmbeddedResource
import scala.math._

/**
 *
 * Utility class to merge jar files, with special handling for
 * META-INF/services/... files.
 */
object JarMerger {

  val serviceFilePredicate = Pattern.compile("META-INF/services/.+").asPredicate()

  def mergeJars(outputFile: File, jarsToMerge: Iterable[File]) {
    val resultJar = new Jar("dummy")
    for (jarFileToMerge <- jarsToMerge) {

      val jarToMerge = new Jar(jarFileToMerge)

      for ((path, resource) <- jarToMerge.getResources().asInstanceOf[java.util.Map[String, Resource]].asScala) {

        if (resultJar.getResources.containsKey(path) && serviceFilePredicate.test(path)) {
          println(s"Merging in ${path} from ${jarFileToMerge.getName}")
          resultJar.putResource(path, mergeServiceFiles(resultJar.getResource(path), resource), true)
        } else {
          resultJar.putResource(path, resource, true)
        }
      }
    }

    resultJar.write(outputFile)
    resultJar.close()
  }

  private def mergeServiceFiles(one: Resource, two: Resource): Resource = {
    val byteArrayOutput = new ByteArrayOutputStream()
    val byteBufferOutput = new BufferedWriter(new OutputStreamWriter(byteArrayOutput))

    val oneLines = Source.fromInputStream(one.openInputStream())(Codec.UTF8).getLines().map(_.trim)
    for (line <- oneLines) {
      if (!line.trim().startsWith("#")) {
        byteBufferOutput.write(line)
        byteBufferOutput.newLine()
      }
    }

    val twoLines = Source.fromInputStream(two.openInputStream())(Codec.UTF8).getLines().map(_.trim)
    for (line <- twoLines) {
      if (!line.trim().startsWith("#")) {
        byteBufferOutput.write(line)
        byteBufferOutput.newLine()
      }
    }

    byteBufferOutput.flush()
    byteBufferOutput.close()

    return new EmbeddedResource(byteArrayOutput.toByteArray(), max(one.lastModified(), two.lastModified()))
  }
}
