package org.fs.chm.ui.swing.webp

import java.awt.image.BufferedImage
import java.awt.image.WritableRaster

import org.fs.utility.StopWatch
import org.slf4s.Logging
import webp.WebpNative

/** WebP decoder - JMI wrapper delegating actual logic to native Google library */
object Webp extends Logging {

  /** Initializes JNI library to speed up subsequent calls */
  def eagerInit(): Unit = {
    WebpNative.getDecoderVersion
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

  def decode(bytes: Array[Byte]): BufferedImage = {
    StopWatch.measureAndCall {
      val width  = new Array[Int](1)
      val height = new Array[Int](1)
      val raw    = WebpNative.decodeBGRA(bytes, bytes.length, width, height)
      require(width(0) > 0, "Failed to decode WebP image, returned width " + width(0))

      val image  = new BufferedImage(width(0), height(0), BufferedImage.TYPE_4BYTE_ABGR)
      val stride = width(0) * 4
      fillImageRaster(
        image.getRaster,
        ((x: Int, y: Int, pixel: Array[Int]) => {
          pixel(2) = raw(stride * y + x * 4 + 0).toInt
          pixel(1) = raw(stride * y + x * 4 + 1).toInt
          pixel(0) = raw(stride * y + x * 4 + 2).toInt
          pixel(3) = raw(stride * y + x * 4 + 3).toInt
          pixel
        })
      )
      image
    }((_, t) => log.debug(s"Image created in $t ms"))
  }

  private def fillImageRaster(raster: WritableRaster, visitor: IPixelVisitor): Unit = {
    val pixel = new Array[Int](4)
    for {
      x <- 0 until raster.getWidth
      y <- 0 until raster.getHeight
    } {
      val res = visitor.visit(x, y, pixel)
      raster.setPixel(x, y, res)
    }
  }

  private trait IPixelVisitor {
    def visit(x: Int, y: Int, pixel: Array[Int]): Array[Int]
  }
}
