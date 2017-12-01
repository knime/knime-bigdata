package com.knime.tpbuilder

import org.osgi.framework.Version
import java.io.File
import scala.collection.mutable.HashMap
import scala.collection.Map
import scala.collection.Set
import scala.collection.mutable.HashSet
import com.knime.tpbuilder.TPConfigReader.TPConfig

case class License(
  name: String,
  url: String,
  distribution: String,
  comments: String) {

  override def toString(): String = {
    val buf = new StringBuilder()
    buf ++= name

    if (url != null) {
      buf ++= " "
      buf ++= url
    }

    if (distribution != null) {
      buf ++= s" (${distribution})"
    }

    buf.toString()
  }
}

case class BundleInfo(
  bundleSymbolicName: String,
  bundleVersion: Version,
  isPrebundled: Boolean,
  licenses: Seq[License],
  vendor: Option[String],
  docUrl: Option[String])

case class Artifact(
    group: String,
    artifact: String,
    version: String,
    packaging: String = "jar",
    classifier: Option[String] = None,
    bundle: Option[BundleInfo] = None,
    file: Option[File] = None,
    sourceFile: Option[File] = None,
    isMerged: Boolean = false,
    mergedArtifacts: Option[Set[Artifact]] = None) {

  override def toString = mvnCoordinate

  def mvnCoordinate(): String = {
    this match {
      case Artifact(group, artifact, version, packaging, None, _, _, _, _, _) =>
        "%s:%s:%s:%s".format(group, artifact, packaging, version)
      case Artifact(group, artifact, version, packaging, Some(classifier), _, _, _, _, _) =>
        "%s:%s:%s:%s".format(group, artifact, classifier, version)
    }
  }
}

object Artifact {
  def fromMvnCoordinate(mvnCoord: String): Artifact = {
    mvnCoord.split(":") match {
      case Array(group, artifact, packaging, version) =>
        Artifact(group, artifact, version, packaging)
      case Array(group, artifact, packaging, classifier, version) =>
        Artifact(group, artifact, version, packaging, Some(classifier))
    }
  }
}


case class ResolutionGroup(name: String,
  toResolve: Set[Artifact])

object Base {

  def assertValidDepGraph(depGraph: Map[Artifact, Set[Artifact]], config: TPConfig) = {
    // first we check that we don't have multiple artifacts that map to the same bundle symbolic name and version
    val bundle2Arts = HashMap[String, HashSet[Artifact]]()
    for (art <- depGraph.keys) {
      bundle2Arts.getOrElseUpdate(art.bundle.get.bundleSymbolicName + "-" + art.bundle.get.bundleVersion, HashSet()).add(art)
    }
    for ((bundleStr, arts) <- bundle2Arts.iterator) {
      assert(arts.size == 1,
        "Multiple maven artifacts map to the same bundle: " + arts.map(_.mvnCoordinate).mkString(", "))
    }
  }
}