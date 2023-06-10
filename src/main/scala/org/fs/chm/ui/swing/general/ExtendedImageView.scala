package org.fs.chm.ui.swing.general

import java.awt.Image
import java.awt.image.BufferedImage
import java.io.File
import java.net.URL
import java.nio.file.Files

import javax.imageio.ImageIO
import javax.swing.text.Element

import com.twelvemonkeys.imageio.stream.ByteArrayImageInputStream

/**
 * ImageView extended to support WebP format.
 */
class ExtendedImageView(el: Element) extends BaseImageView(el) {

  override protected def createImageFromUrl(src: URL): Image = {
    require(src.getProtocol == "file", "ExtendedImageView is made to support local images only!")
    val file = new File(src.getFile)
    require(file.exists(), s"File ${file.getAbsolutePath} doesn't exist!")
    val bytes = Files.readAllBytes(file.toPath)

    // Reading through ImageIO first for the formats not supported by AWT
    // (just WebP for the moment)
    ExtendedImageView.imageioTryReadBytes(bytes).getOrElse(toolkit.createImage(bytes))
  }
}

object ExtendedImageView {
  protected def imageioTryReadBytes(bytes: Array[Byte]): Option[BufferedImage] = {
    Option(ImageIO.read(new ByteArrayImageInputStream(bytes)))
  }

  def main(args: Array[String]): Unit = {
    import javax.swing.ImageIcon
    import javax.swing.JLabel
    import javax.swing.JOptionPane

    val path = "_data/webp/test_sticker.webp"
    val imageBytes = Files.readAllBytes(new File(path).toPath)
    val image = imageioTryReadBytes(imageBytes).get
    val imageWrapper = new JLabel(new ImageIcon(image))
    JOptionPane.showMessageDialog(null, imageWrapper, "Image preview", JOptionPane.PLAIN_MESSAGE, null);
  }
}
