addSbtPlugin("com.eed3si9n"  % "sbt-assembly"  % "2.1.1")
addSbtPlugin("com.eed3si9n"  % "sbt-buildinfo" % "0.11.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.7")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.11"

// Launch4j
libraryDependencies += ("net.sf.launch4j" % "launch4j" % "3.12")
  .exclude("com.ibm.icu", "icu4j")
  .exclude("abeille", "net.java.abeille")

// Launch4j dependency - required for com.springsource.org.apache.batik
resolvers ++= Seq(
  "SpringSource" at "https://repository.springsource.com/maven/bundles/external",
  "Simulation @ TU Delft" at "https://simulation.tudelft.nl/maven/"
)
