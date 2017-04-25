package com.knime.tpbuilder

import scala.collection.{ Map, Set }
import scala.collection.mutable.HashSet
import com.knime.tpbuilder.TPConfigReader.TPConfig
import com.knime.tpbuilder.TPConfigReader.TPArtifactGroup

object DepGraphPrinter {

  def print(depGraph: Map[Artifact, Set[Artifact]], config: TPConfig) = {
    val alreadyPrinted = HashSet[String]()

    for (resGroup <- config.artifactGroups) {
      for (rootArt <- TPArtifactGroup.toArtifactSet(resGroup)) {
        printRecursively(rootArt, 0, depGraph, alreadyPrinted)
      }
    }
  }

  private def printRecursively(art: Artifact, level: Int, depGraph: Map[Artifact, Set[Artifact]], alreadyPrinted: HashSet[String]): Unit = {
    val mvnCoordinate = art.mvnCoordinate
    
    if (depGraph.contains(art))
      println(("." * level) + mvnCoordinate)
    else
      println(("." * level) + mvnCoordinate + " (not bundled due to exclusion)")

    val artDeps = depGraph.getOrElse(art, Set())
    
    if (alreadyPrinted(mvnCoordinate)) {
      if (!artDeps.isEmpty)
        println(("." * (level + 1)) + "(subtree already printed above)")
    } else {
      alreadyPrinted.add(mvnCoordinate)
      for (dep <- artDeps) {
        printRecursively(dep, level + 1, depGraph, alreadyPrinted)
      }
    }
  }
}