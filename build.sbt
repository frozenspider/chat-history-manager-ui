name           := "chat-history-manager-ui"
version        := "0.3-SNAPSHOT"
homepage       := Some(url("https://github.com/frozenspider/chat-history-manager-ui"))
scalaVersion   := "2.13.10"

// Show tests duration and full stacktrace on test errors
Test / testOptions  += Tests.Argument("-oDF")

// Disable concurrent test execution
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

Compile / sourceManaged  := baseDirectory.value / "src_managed" / "main" / "scala"
Test    / sourceManaged  := baseDirectory.value / "src_managed" / "test" / "scala"

Compile / run / mainClass := Some("org.fs.chm.Main")

// Reload sbt project when build.sbt changed
Global / onChangedBuildSource := ReloadOnSourceChanges

// ScalaPB config
val protobufDirs = settingKey[(File, Seq[File])]("Path to the protobuf files")
protobufDirs := {
  def fail(msg: String) = throw new sbt.MessageOnlyException(s"Can't compile protobufs: $msg")
  val rustProjectFileName = "rust-project-path.txt"
  val pbConfigFile = baseDirectory.value / rustProjectFileName
  if (!pbConfigFile.exists) {
    fail(s"./${rustProjectFileName} does not exist! Create one :)")
  }
  val rustPath = IO.readLines(pbConfigFile).headOption.getOrElse(fail(s"./${rustProjectFileName} is empty!"))

  val pbPaths = for (pbPath <- Seq("core/protobuf", "backend/protobuf")) yield {
    val pbPathFile = new File(rustPath, pbPath)
    if (!pbPathFile.exists || !pbPathFile.isDirectory) {
      fail(s"${pbPath} aka ${pbPathFile.getAbsolutePath} (specified in /${rustProjectFileName}) does not exist or is not a directory!")
    }
    if (pbPathFile.list((_, n) => n.toLowerCase.endsWith(".proto")).isEmpty) {
      fail(s"${pbPath} aka ${pbPathFile.getAbsolutePath} (specified in /${rustProjectFileName}) has no *.proto files!")
    }
    pbPathFile
  }
  (new File(rustPath), pbPaths)
}

Compile / PB.deleteTargetDirectory := true
Compile / PB.includePaths := Seq(protobufDirs.value._1, baseDirectory.value / "target" / "protobuf_external")
Compile / PB.protoSources := protobufDirs.value._2

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](
      name,
      version,
      homepage,
      "fullName" -> (name.value + " v" + version.value)
    ),
    buildInfoOptions ++= Seq(
      BuildInfoOption.BuildTime
    ),
    buildInfoPackage := "org.fs.chm",
    buildInfoUsePackageAsPath := true
  )

resolvers += "jitpack" at "https://jitpack.io"

val scalapbVer = scalapb.compiler.Version.scalapbVersion
val grpcJavaVer = scalapb.compiler.Version.grpcJavaVersion

// Regular dependencies
libraryDependencies ++= Seq(
  // Logging
  "ch.timo-schmid"                %% "slf4s-api"                  % "1.7.30.2",
  "org.slf4j"                     %  "jcl-over-slf4j"             % "1.7.36",
  "ch.qos.logback"                %  "logback-classic"            % "1.1.2",
  // UI
  "org.scala-lang.modules"        %% "scala-swing"                % "3.0.0",
  "org.jdatepicker"               %  "jdatepicker"                % "1.3.4",
  // Audio
  "org.gagravarr"                 %  "vorbis-java-core"           % "0.8",
  "org.gagravarr"                 %  "vorbis-java-tika"           % "0.8",
  "com.googlecode.soundlibs"      %  "vorbisspi"                  % "1.0.3.3",
  "com.googlecode.soundlibs"      %  "mp3spi"                     % "1.9.5.4",
  "com.github.lostromb.concentus" %  "Concentus"                  % "fdf276ed6b",
  // Images
  "com.twelvemonkeys.imageio"     % "imageio-webp"                % "3.9.4",
  // Utility
  "com.github.frozenspider"       %% "fs-common-utils"            % "0.2.0",
  "commons-codec"                 %  "commons-codec"              % "1.16.0",
  "commons-io"                    %  "commons-io"                 % "2.13.0",
  "org.apache.commons"            %  "commons-lang3"              % "3.12.0",
  "com.github.nscala-time"        %% "nscala-time"                % "2.32.0",
  "org.scala-lang.modules"        %% "scala-parallel-collections" % "1.0.4",
  // Other
  "com.typesafe"                  %  "config"                     % "1.4.2",
  "org.scala-lang.modules"        %% "scala-parser-combinators"   % "2.2.0",
  // Protobuf
  "com.thesamet.scalapb"          %% "scalapb-runtime"            % scalapbVer % "protobuf",
  "com.thesamet.scalapb"          %% "scalapb-runtime-grpc"       % scalapbVer,
  "io.grpc"                       %  "grpc-netty"                 % grpcJavaVer,
  "io.grpc"                       % "grpc-services"               % grpcJavaVer,
  // Test
  "junit"                         %  "junit"                      % "4.12"     % "test",
  "org.scalactic"                 %% "scalactic"                  % "3.2.15"   % "test",
  "org.scalatest"                 %% "scalatest"                  % "3.2.15"   % "test",
  "org.scalatestplus"             %% "junit-4-13"                 % "3.2.15.0" % "test"
)
