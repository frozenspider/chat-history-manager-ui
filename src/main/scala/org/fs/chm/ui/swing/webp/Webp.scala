package org.fs.chm.ui.swing.webp

import java.awt.Point
import java.awt.Transparency
import java.awt.color.ColorSpace
import java.awt.image.BufferedImage
import java.awt.image.ColorModel
import java.awt.image.ComponentColorModel
import java.awt.image.DataBuffer
import java.awt.image.DataBufferByte
import java.awt.image.Raster

import org.fs.utility.StopWatch
import org.slf4s.Logging
import webp.WebpNative

/** WebP decoder - JMI wrapper delegating actual logic to native Google library */
object Webp extends Logging {

  lazy val webNativeOption: Option[WebpNative.type] = {
    try {
      val result = Some(WebpNative)
      result map (_.getDecoderVersion)
      result
    } catch {
      case ex: Exception =>
        log.warn("WebP library loading failed!", ex)
        None
      case ex: UnsatisfiedLinkError =>
        log.warn("WebP library loading failed!", ex)
        None
    }
  }

  /** Initializes JNI library to speed up subsequent calls */
  def eagerInit(): Unit = {
    webNativeOption
  }

  /** Check whether given raw bytes start with a WEBP image header */
  def isWebp(bytes: Array[Byte]): Boolean = {
    /*
     * Header format:
     *
     * R I F F
     * <4 bytes file size>
     * W E B P
     */
    (bytes.length >= 12) &&
    (bytes.take(4) sameElements "RIFF".getBytes) &&
    (bytes.slice(8, 12) sameElements "WEBP".getBytes)
  }

  private val colorModel: ColorModel = new ComponentColorModel(
    ColorSpace.getInstance(ColorSpace.CS_sRGB),
    Array(8, 8, 8, 8),
    true /* hasAlpha */,
    false /*isAlphaPremultiplied */,
    Transparency.TRANSLUCENT,
    DataBuffer.TYPE_BYTE
  )

  def decode(bytes: Array[Byte]): BufferedImage =
    StopWatch.measureAndCall {
      val width   = new Array[Int](1)
      val height  = new Array[Int](1)
      val rawBGRA = webNativeOption map { webNative =>
        webNative.decodeBGRA(bytes, bytes.length, width, height)
      } getOrElse {
        width(0) = 1
        height(0) = 1
        Array.fill[Byte](4)(0)
      }
      require(width(0) > 0, "Failed to decode WebP image, returned width " + width(0))

      val dataBuffer = new DataBufferByte(rawBGRA, rawBGRA.length)
      val raster = Raster.createInterleavedRaster(
        dataBuffer,
        width(0),
        height(0),
        width(0) * 4 /* scanlineStride */,
        4 /*pixelStride */,
        Array(2, 1, 0, 3) /* bandOffsets, mapping from BGRA to RGBA */,
        new Point(0, 0)
      )
      new BufferedImage(colorModel, raster, false, null)
    }((_, t) => log.debug(s"WebP image created in $t ms"))
}
