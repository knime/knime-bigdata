package com.knime.tpbuilder.aether

import java.io.File

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.mutable.Set

import org.eclipse.aether.RepositorySystemSession
import org.eclipse.aether.artifact.{ Artifact => AetherArtifact }
import org.eclipse.aether.repository.RemoteRepository
import org.eclipse.aether.resolution.ArtifactRequest
import org.eclipse.aether.resolution.ArtifactResolutionException
import org.eclipse.aether.util.artifact.SubArtifact

import com.knime.tpbuilder.Artifact
import com.knime.tpbuilder.TPConfigReader.TPConfig
import com.knime.tpbuilder.maven.MavenHelper
import com.knime.tpbuilder.osgi.OsgiUtil
import com.knime.tpbuilder.License

object DepGraphResolver {

  def resolveDepGraph(depGraph: Map[Artifact, Set[Artifact]], config: TPConfig): Map[Artifact, Set[Artifact]] = {
    val oldGraph = HashMap[Artifact, Set[Artifact]]()
    oldGraph ++= depGraph

    depGraph.clear()

    // val remoteRepos = Seq(AetherUtils.mavenCentral).asJava
    val session = AetherUtils.repoSession

    val resolvedArtifactCache = HashMap[String, Artifact]()
    val artsWithSource = TPConfig.getArtifactCoordsWithSource(oldGraph, config)
    val resolve = doResolve(session, config, resolvedArtifactCache, artsWithSource)(_)

    for (oldArt <- oldGraph.keys) {
      depGraph += (resolve(oldArt) -> oldGraph(oldArt).map(resolve))
    }

    depGraph
  }

  private def doResolve(session: RepositorySystemSession, 
      config: TPConfig, 
      resolvedArtifactCache: Map[String, Artifact], 
      artsWithSource: Set[Artifact])
  (art: Artifact): Artifact = {

    val artCoord = art.mvnCoordinate

    resolvedArtifactCache.get(artCoord) match {
      case Some(resolvedArt) => resolvedArt
      case None => {
        println(s"  Resolving ${artCoord}")

        val remoteRepos = AetherUtils.remoteArtifactRepos(artCoord).toSeq

        val aetherArt = AetherUtils.artToAetherArt(art)
        val result = AetherUtils.repoSys.resolveArtifact(session,
          new ArtifactRequest(aetherArt, remoteRepos.asJava, null))

        val licenses = getLicenses(result.getArtifact, config)
          
        val resolvedArt = OsgiUtil.withBundleAndVersion(AetherUtils.aetherArtToArt(result.getArtifact), None, licenses, config)
        println(s"    Bundle-SymbolicName ${resolvedArt.bundle.get.bundleSymbolicName} / Bundle-Version: ${resolvedArt.bundle.get.bundleVersion}")

        // only grab the source for jars where this is explicitly requested
        val sourceFile = if (config.source.get && artsWithSource(art)) {
            println(s"    Bundle-SymbolicName ${resolvedArt.bundle.get.bundleSymbolicName}.source / Bundle-Version: ${resolvedArt.bundle.get.bundleVersion}")
            trySourceResolution(aetherArt, remoteRepos, session)
          } else None

        val resolvedArtWithOptionalSource = OsgiUtil.withBundle(resolvedArt, resolvedArt.bundle.get, resolvedArt.file, sourceFile)

        resolvedArtifactCache += (artCoord -> resolvedArtWithOptionalSource)

        resolvedArtWithOptionalSource
      }
    }
  }

  private def trySourceResolution(aetherArt: AetherArtifact, remoteRepos: Seq[RemoteRepository], session: RepositorySystemSession): Option[File] = {

    try {
      val sourceAetherart = new SubArtifact(aetherArt, "*-sources", "jar")
      val sourceResult = AetherUtils.repoSys.resolveArtifact(session, new ArtifactRequest(sourceAetherart, remoteRepos.asJava, null))
      return Some(sourceResult.getArtifact.getFile)
    } catch {
      case e: ArtifactResolutionException => {} // do nothing for now
    }

    try {
      val sourceAetherart = new SubArtifact(aetherArt, "*-sources", "src")
      val sourceResult = AetherUtils.repoSys.resolveArtifact(session, new ArtifactRequest(sourceAetherart, remoteRepos.asJava, null))
      return Some(sourceResult.getArtifact.getFile)
    } catch {
      case e: ArtifactResolutionException => {
        System.err.println(s"    Could not resolve source for: ${AetherUtils.aetherArtToArt(aetherArt).mvnCoordinate}")
        return None
      }
    }
  }

  private def getLicenses(art: AetherArtifact, config: TPConfig): Seq[License] = {
    TPConfig.getLicense(config)(AetherUtils.aetherArtToArt(art)) match {
      case Some(license) => Seq(license)
      case _ => MavenHelper.getLicenses(art).map(l => License(l.getName, l.getUrl, l.getDistribution, l.getComments))
    }
  }
}
