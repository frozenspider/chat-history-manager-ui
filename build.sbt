name           := "chat-history-manager"
version        := "0.1-SNAPSHOT"
homepage       := Some(url("https://github.com/frozenspider/chat-history-manager"))
scalaVersion   := "2.13.10"

// Show tests duration and full stacktrace on test errors
Test / testOptions  += Tests.Argument("-oDF")

// Disable concurrent test execution
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

// sourceManaged            := baseDirectory.value / "src_managed"
Compile / sourceManaged  := baseDirectory.value / "src_managed" / "main" / "scala"
Test / sourceManaged     := baseDirectory.value / "src_managed" / "test" / "scala"

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
  "commons-codec"                 %  "commons-codec"              % "1.15",
  "org.apache.commons"            %  "commons-lang3"              % "3.12.0",
  "com.github.nscala-time"        %% "nscala-time"                % "2.32.0",
  "org.scala-lang.modules"        %% "scala-parallel-collections" % "1.0.4",
  // Database
  "org.tpolecat"                  %% "doobie-core"                % "1.0.0-RC2",
  "org.tpolecat"                  %% "doobie-h2"                  % "1.0.0-RC2",
  "org.flywaydb"                  %  "flyway-core"                % "6.1.3",
  // Other
  "org.json4s"                    %% "json4s-jackson"             % "4.1.0-M2",
  "org.json4s"                    %% "json4s-ext"                 % "4.1.0-M2",
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
