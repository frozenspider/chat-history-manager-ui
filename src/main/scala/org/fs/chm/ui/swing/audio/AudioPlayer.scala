package org.fs.chm.ui.swing.audio

import java.io.File

import org.slf4s.Logging

class AudioPlayer(
    val file: File,
    mimeTypeOption: Option[String],
    providedDurationOption: Option[Int]
) extends Logging {

  private var fileLoaded: Either[String, Boolean] = Right(false)

  //  private val outputBitrate       = 44100
  //  private val outputAudioFormat   = new AudioFormat(outputBitrate, 16, 1, true, false)
  //  private val outputAudioDataInfo = new DataLine.Info(classOf[Clip], outputAudioFormat)

  def isLoaded: Boolean = fileLoaded.isLeft || fileLoaded.right.toOption.get

  def errorMessage = fileLoaded.left.toOption.get

  lazy val fileDetailsOption: Option[AudioFileDetails] = {
    try {
      // This stuff is an attempt to play Ogg+Opus files via pure Java.
      // A failed one, I might add.
      /*
      val bytes = Files.readAllBytes(file.toPath)
      if (new String(bytes.take(4)) == "OggS") {
        val oggFile = new OggFile(new ByteArrayInputStream(bytes))

        val packetReader = oggFile.getPacketReader
        val firstPacket  = packetReader.getNextPacket
        packetReader.unreadPacket(firstPacket)

        val details = OggStreamIdentifier.identifyType(firstPacket) match {
          case OggStreamIdentifier.OPUS_AUDIO =>
            val of          = new OpusFile(packetReader)
            val stats       = new OpusStatistics(of)
            val dur         = stats.getDurationSeconds
            val decoder     = new OpusDecoder(OpusAudioData.OPUS_GRANULE_RATE, 1)
            val firstPacket = of.getNextAudioPacket
            val data = (firstPacket #:: Stream.continually(of.getNextAudioPacket))
              .takeWhile(_ != null)
              .map(_.getData)
              .toArray
              .flatten
//            val data = Stream.continually(packetReader.getNextPacket)
//              .takeWhile(_ != null)
//              .map(_.getData)
//              .toArray
//              .flatten
            val buf       = Array.ofDim[Byte](100500)
            val frameSize = 960 //firstPacket.getNumberOfSamples / firstPacket.getNumberOfFrames
            val len       = decoder.decode(data, 0, data.length, buf, 0, frameSize, false)
            println(len)
            // Doesn't work
            val clip = AudioSystem.getLine(outputAudioDataInfo).asInstanceOf[Clip]
            fileLoaded = Right(true)
            Some(AudioFileDetails(duration = ???))
          //case etc => throw new IllegalArgumentException("Unsupported type " + etc)
        }
        details
      } else {
        val msg = mimeTypeOption match {
          case Some(mime) => s"Audio files of type $mime not supported"
          case None       => s"Unable to recognize audio type"
        }
        fileLoaded = Left(msg)
        None
      }
       */
      fileLoaded = Right(true)
      // For now, screw all this
      providedDurationOption map AudioFileDetails.apply
    } catch {
      case ex: Exception =>
        log.warn(s"Failed to load file ${file.getAbsolutePath}", ex)
        fileLoaded = Left("Failed to load audio file")
        None
    }
  }

  /**
   * None if file not found, or if duration is not provided and file format not recognized.
   * Does not cause file to load if duration is provided explicitly.
   */
  val durationOption: Option[Double] =
    if (!file.exists) {
      None
    } else {
      providedDurationOption map (_.toDouble) orElse {
        // This will cause actual file loading
        fileDetailsOption map (_.duration)
      }
    }
}

case class AudioFileDetails(
    duration: Int
) {}
