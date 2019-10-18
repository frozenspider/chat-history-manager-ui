package org.fs.chm.ui.swing.general

import java.awt.Image
import java.io.File
import java.net.URL
import java.nio.file.Files

import javax.swing.text.Element
import org.fs.chm.ui.swing.webp.Webp

/**
 * ImageView extended to support WebP format.
 */
class ExtendedImageView(el: Element) extends BaseImageView(el) {

  override protected def createImageFromUrl(src: URL): Image = {
    require(src.getProtocol == "file", "ExtendedImageView is made to support local images only!")
    val file = new File(src.getFile)
    require(file.exists(), s"File ${file.getAbsolutePath} doesn't exist!")
    val bytes = Files.readAllBytes(file.toPath)

    // Special treatment for WebP format as it's not supported by AWT toolkit
    if (Webp.isWebp(bytes)) {
      Webp.decode(bytes)
    } else {
      toolkit.createImage(bytes)
    }
  }

}
