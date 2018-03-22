package com.knime.tpbuilder.aether

import java.io.File
import java.nio.file.Paths
import java.util.Collections

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.Set
import scala.collection.mutable.HashMap
import scala.collection.mutable.{ Set => MutableSet }

import org.apache.maven.artifact.resolver.ArtifactResolver
import org.apache.maven.repository.internal.MavenRepositorySystemUtils
import org.apache.maven.settings.Mirror
import org.apache.maven.settings.Settings
import org.apache.maven.settings.building.DefaultSettingsBuilderFactory
import org.apache.maven.settings.building.DefaultSettingsBuildingRequest
import org.eclipse.aether.DefaultRepositorySystemSession
import org.eclipse.aether.RepositorySystem
import org.eclipse.aether.artifact.{ Artifact => AetherArtifact }
import org.eclipse.aether.artifact.DefaultArtifact
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory
import org.eclipse.aether.graph.Dependency
import org.eclipse.aether.impl.ArtifactDescriptorReader
import org.eclipse.aether.impl.RemoteRepositoryManager
import org.eclipse.aether.impl.VersionRangeResolver
import org.eclipse.aether.repository.Authentication
import org.eclipse.aether.repository.AuthenticationContext
import org.eclipse.aether.repository.AuthenticationDigest
import org.eclipse.aether.repository.LocalRepository
import org.eclipse.aether.repository.MirrorSelector
import org.eclipse.aether.repository.RemoteRepository
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory
import org.eclipse.aether.spi.connector.transport.TransporterFactory
import org.eclipse.aether.transport.file.FileTransporterFactory
import org.eclipse.aether.transport.http.HttpTransporterFactory
import org.eclipse.aether.util.repository.DefaultMirrorSelector

import com.knime.tpbuilder.Artifact
import com.knime.tpbuilder.TPConfigReader
import com.knime.tpbuilder.TPConfigReader.TPConfig

object AetherUtils {
  val localRepo = new LocalRepository(Paths.get(System.getProperty("user.home"), ".m2", "repository").toFile())

  private val mavenCentral = new RemoteRepository.Builder("maven-central", "default", "http://repo1.maven.org/maven2/").build()
  
  private val locator = MavenRepositorySystemUtils.newServiceLocator()

  val repoSys = {
    locator.addService(classOf[RepositoryConnectorFactory], classOf[BasicRepositoryConnectorFactory])
    locator.addService(classOf[TransporterFactory], classOf[FileTransporterFactory])
    locator.addService(classOf[TransporterFactory], classOf[HttpTransporterFactory])

    locator.getService(classOf[RepositorySystem])
  }

  val artifactResolver = locator.getService(classOf[ArtifactResolver])
  
  val versionRangeResolver = locator.getService(classOf[VersionRangeResolver])
  
  val remoteRepositoryResolver = locator.getService(classOf[RemoteRepositoryManager])
  
  val artDescriptorReader = locator.getService(classOf[ArtifactDescriptorReader])
  
  val repoSession = {
    val session = MavenRepositorySystemUtils.newSession().asInstanceOf[DefaultRepositorySystemSession]
    session.setLocalRepositoryManager(repoSys.newLocalRepositoryManager(session, AetherUtils.localRepo))
    session
  }

  /**
   * Collects information which remote repositories were used to resolve the metadata
   * of a maven artifact.
   */
  val remoteArtifactRepos = HashMap[String, Set[RemoteRepository]]()
  
  val defaultRemoteArtifactRepos: Set[RemoteRepository] = MutableSet()
  
  def initDefaultRemoteArtifactRepos(config: TPConfig) = {
    val settings = getMavenSettings()

    val mirrorSelector = createMirrorSelector(Option(settings.getMirrors).getOrElse(Collections.emptyList()))
    
    val repos = MutableSet[RemoteRepository]()
    
    repos.add(useMirrorIfNecessary(mavenCentral, mirrorSelector))
     
    for (repo <- config.mavenRepositories) {
      val remoteRepo = useMirrorIfNecessary(
          new RemoteRepository.Builder(repo.id, "default", repo.url).build(),
          mirrorSelector)
      repos.add(remoteRepo)
    }
    
    defaultRemoteArtifactRepos.asInstanceOf[MutableSet[RemoteRepository]] ++= repos.map(withAuthentication(_, settings))
  }

  private def useMirrorIfNecessary(repo: RemoteRepository, mirrorSelector: MirrorSelector): RemoteRepository = {
    Option(mirrorSelector.getMirror(repo)) match {
      case Some(mirror) =>
        new RemoteRepository.Builder(mirror).setMirroredRepositories(Collections.emptyList()).build()
      case _ =>
        repo
    }
  }

  private def withAuthentication(repo: RemoteRepository, settings: Settings): RemoteRepository = {
    Option(settings.getServer(repo.getId)) match {
      case Some(server) =>
        new RemoteRepository.Builder(repo)
          .setAuthentication(
              new Authentication() {
                def fill(context: AuthenticationContext, key: String, data: java.util.Map[String, String]) = {
                  context.put(AuthenticationContext.USERNAME, server.getUsername)
                  context.put(AuthenticationContext.PASSWORD, server.getPassword)
                }
                
                def digest(digest: AuthenticationDigest) = {
                  digest.update(AuthenticationContext.USERNAME, server.getUsername)
                  digest.update(AuthenticationContext.PASSWORD, server.getPassword)
                }
              })
          .build()
      case _ => repo
    }
  }
  
  private def createMirrorSelector(mirrors: java.util.List[Mirror]): MirrorSelector = {
    val mirrorSelector = new DefaultMirrorSelector()

    for (mirror <- mirrors.asScala) {
      mirrorSelector.add(mirror.getId(),
        mirror.getUrl(),
        mirror.getLayout(),
        false,
        mirror.getMirrorOf(),
        mirror.getMirrorOfLayouts());
    }

    mirrorSelector
  }

  def depToArt(dep: Dependency): Artifact = {
    val aetherArt = dep.getArtifact
    aetherArtToArt(aetherArt)
  }

  def artToAetherArt(art: Artifact): AetherArtifact = {
    new DefaultArtifact(
      art.group,
      art.artifact,
      art.classifier.getOrElse(""),
      "jar",
      art.version)
  }
  
  def artToDep(art: Artifact): Dependency = {
    new Dependency(new DefaultArtifact(
      art.group,
      art.artifact,
      art.classifier.getOrElse(""),
      "jar",
      art.version),
      "runtime")
  }


  def aetherArtToArt(aetherArt: AetherArtifact): Artifact = {
    val art = Artifact(group = aetherArt.getGroupId,
      artifact = aetherArt.getArtifactId,
      version = aetherArt.getBaseVersion,
      classifier =
        if (aetherArt.getClassifier.length() > 0)
          Some(aetherArt.getClassifier)
        else None,
      file = Option(aetherArt.getFile))
    art
  }
  
  def getMavenSettings(): Settings = {
    val userSettings = new File(String.join(File.separator,
        Seq(System.getProperty("user.home" ), ".m2", "settings.xml").asJava))
    
    val globalSettings = new File(String.join(File.separator,
        Seq(System.getProperty("maven.home",  Option(System.getenv("M2_HOME")).getOrElse("")),
        "conf",
        "settings.xml").asJava))

    val settingsBuildingRequest = new DefaultSettingsBuildingRequest()
      .setSystemProperties(System.getProperties())
      .setUserSettingsFile(userSettings)
      .setGlobalSettingsFile(globalSettings)
      
    val settings = new DefaultSettingsBuilderFactory()
      .newInstance()
      .build(settingsBuildingRequest)
      .getEffectiveSettings
      
    settings
  }
}