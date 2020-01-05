name           := "chat-history-manager"
version        := "0.1-SNAPSHOT"
homepage       := Some(url("https://github.com/frozenspider/chat-history-manager"))
scalaVersion   := "2.12.10"

// Show tests duration and full stacktrace on test errors
testOptions in Test += Tests.Argument("-oDF")

// Disable concurrent test execution
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

sourceManaged            := baseDirectory.value / "src_managed"
sourceManaged in Compile := baseDirectory.value / "src_managed" / "main" / "scala"
sourceManaged in Test    := baseDirectory.value / "src_managed" / "test" / "scala"

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

// Regular dependencies
libraryDependencies ++= Seq(
  // Network
  "org.apache.httpcomponents"     %  "httpclient"               % "4.5.5",
  // Logging
  "org.slf4s"                     %% "slf4s-api"                % "1.7.25",
  "org.slf4j"                     %  "jcl-over-slf4j"           % "1.7.25",
  "ch.qos.logback"                %  "logback-classic"          % "1.1.2",
  // UI
  "org.scala-lang.modules"        %% "scala-swing"              % "2.1.1",
  // Audio
  "org.gagravarr"                 %  "vorbis-java-core"         % "0.8",
  "org.gagravarr"                 %  "vorbis-java-tika"         % "0.8",
  "com.googlecode.soundlibs"      %  "vorbisspi"                % "1.0.3.3",
  "com.googlecode.soundlibs"      %  "mp3spi"                   % "1.9.5.4",
  "com.github.lostromb.concentus" %  "Concentus"                % "fdf276ed6b",
  // WebP codec
  "org.scijava"                   %  "native-lib-loader"        % "2.3.4",
  // Utility
  "com.github.frozenspider"       %% "fs-common-utils"          % "0.1.3",
  "commons-codec"                 %  "commons-codec"            % "1.13",
  "org.apache.commons"            %  "commons-lang3"            % "3.9",
  "com.github.nscala-time"        %% "nscala-time"              % "2.16.0",
  // Database
  "org.tpolecat"                  %% "doobie-core"              % "0.8.8",
  "org.tpolecat"                  %% "doobie-h2"                % "0.8.8",
  "org.flywaydb"                  %  "flyway-core"              % "6.1.3",
  // Other
  "org.json4s"                    %% "json4s-jackson"           % "3.6.7",
  "org.json4s"                    %% "json4s-ext"               % "3.6.7",
  "com.typesafe"                  %  "config"                   % "1.3.2",
  "org.scala-lang.modules"        %% "scala-parser-combinators" % "1.1.1",
  // Test
  "junit"                         %  "junit"                    % "4.12"  % "test",
  "org.scalactic"                 %% "scalactic"                % "3.0.4" % "test",
  "org.scalatest"                 %% "scalatest"                % "3.0.4" % "test",
  "com.google.jimfs"              %  "jimfs"                    % "1.1"   % "test"
)
