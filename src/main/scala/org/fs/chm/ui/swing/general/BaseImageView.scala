package org.fs.chm.ui.swing.general

import java.awt.Color
import java.awt.Container
import java.awt.Graphics
import java.awt.Image
import java.awt.Rectangle
import java.awt.Shape
import java.awt.Toolkit
import java.awt.image.ImageObserver
import java.net.MalformedURLException
import java.net.URL
import java.util.Dictionary

import javax.swing.GrayFilter
import javax.swing.Icon
import javax.swing.ImageIcon
import javax.swing.SwingUtilities
import javax.swing.UIManager
import javax.swing.event.DocumentEvent
import javax.swing.text.AbstractDocument
import javax.swing.text.AttributeSet
import javax.swing.text.BadLocationException
import javax.swing.text.Element
import javax.swing.text.JTextComponent
import javax.swing.text.LayeredHighlighter
import javax.swing.text.Position
import javax.swing.text.Segment
import javax.swing.text.StyledDocument
import javax.swing.text.View
import javax.swing.text.ViewFactory
import javax.swing.text.html.HTML
import javax.swing.text.html.HTMLDocument
import javax.swing.text.html.InlineView
import javax.swing.text.html.StyleSheet

/**
 * Almost all of this class is copy-paste from AWT ImageView, with private stuff made protected instead.
 * Who the hell though that making everything private would be a good idea?
 */
class BaseImageView(el: Element) extends View(el) {

  /**
   * If true, when some of the bits are available a repaint is done.
   * <p>
   * This is set to false as swing does not offer a repaint that takes a
   * delay. If this were true, a bunch of immediate repaints would get
   * generated that end up significantly delaying the loading of the image
   * (or anything else going on for that matter).
   */
  protected val sIsInc = false

  /**
   * Repaint delay when some of the bits are available.
   */
  protected val sIncRate = 100

  /**
   * Property name for pending image icon
   */
  protected val PENDING_IMAGE = "html.pendingImage"

  /**
   * Property name for missing image icon
   */
  protected val MISSING_IMAGE = "html.missingImage"

  /**
   * Document property for image cache.
   */
  protected val IMAGE_CACHE_PROPERTY = "imageCache"

  // Height/width to use before we know the real size, these should at least
  // the size of <code>sMissingImageIcon</code> and
  // <code>sPendingImageIcon</code>
  protected val DEFAULT_WIDTH  = 38
  protected val DEFAULT_HEIGHT = 38

  /**
   * Default border to use if one is not specified.
   */
  protected val DEFAULT_BORDER = 2

  // Bitmask values
  protected val LOADING_FLAG      = 1
  protected val LINK_FLAG         = 2
  protected val WIDTH_FLAG        = 4
  protected val HEIGHT_FLAG       = 8
  protected val RELOAD_FLAG       = 16
  protected val RELOAD_IMAGE_FLAG = 32
  protected val SYNC_LOAD_FLAG    = 64

  protected var attr:          AttributeSet = _
  protected var image:         Image        = _
  protected var disabledImage: Image        = _
  protected var width  = 0
  protected var height = 0

  /** Bitmask containing some of the above bitmask values. Because the
   * image loading notification can happen on another thread access to
   * this is synchronized (at least for modifying it). */
  protected var state:       Int       = RELOAD_FLAG | RELOAD_IMAGE_FLAG
  protected var container:   Container = _
  protected var fBounds:     Rectangle = new Rectangle
  protected var borderColor: Color     = _
  // Size of the border, the insets contains this valid. For example, if
  // the HSPACE attribute was 4 and BORDER 2, leftInset would be 6.
  protected var borderSize: Short = 0
  // Insets, obtained from the painter.
  protected var leftInset:   Short = 0
  protected var rightInset:  Short = 0
  protected var topInset:    Short = 0
  protected var bottomInset: Short = 0

  /**
   * We don't directly implement ImageObserver, instead we use an instance
   * that calls back to us.
   */
  protected val imageObserver = new ImageHandler

  /**
   * Used for alt text. Will be non-null if the image couldn't be found,
   * and there is valid alt text.
   */
  protected var altView: View = _

  /** Alignment along the vertical (Y) axis. */
  protected var vAlign: Float = 0.0f

  protected lazy val toolkit: Toolkit = {
    Toolkit.getDefaultToolkit
  }

  /**
   * Returns the text to display if the image can't be loaded. This is
   * obtained from the Elements attribute set with the attribute name
   * <code>HTML.Attribute.ALT</code>.
   */
  def getAltText: String = {
    getElement.getAttributes.getAttribute(HTML.Attribute.ALT).asInstanceOf[String]
  }

  /**
   * Return a URL for the image source,
   * or null if it could not be determined.
   */
  def getImageURL: URL = {
    val src = getElement.getAttributes.getAttribute(HTML.Attribute.SRC).asInstanceOf[String]
    if (src == null) {
      null
    } else {
      val reference = getDocument.asInstanceOf[HTMLDocument].getBase
      try {
        new URL(reference, src)
      } catch {
        case e: MalformedURLException => null
      }
    }
  }

  /**
   * Returns the icon to use if the image couldn't be found.
   */
  def getNoImageIcon: Icon = UIManager.getLookAndFeelDefaults.get(MISSING_IMAGE).asInstanceOf[Icon]

  /**
   * Returns the icon to use while in the process of loading the image.
   */
  def getLoadingImageIcon: Icon = UIManager.getLookAndFeelDefaults.get(PENDING_IMAGE).asInstanceOf[Icon]

  /**
   * Returns the image to render.
   */
  def getImage: Image = {
    sync()
    image
  }

  protected def getImage(enabled: Boolean): Image = {
    var img = getImage
    if (!enabled) {
      if (disabledImage == null) {
        disabledImage = GrayFilter.createDisabledImage(img)
      }
      img = disabledImage
    }
    img
  }

  /**
   * Sets how the image is loaded. If <code>newValue</code> is true,
   * the image we be loaded when first asked for, otherwise it will
   * be loaded asynchronously. The default is to not load synchronously,
   * that is to load the image asynchronously.
   */
  def setLoadsSynchronously(newValue: Boolean): Unit = this.synchronized {
    if (newValue) {
      state |= SYNC_LOAD_FLAG
    } else {
      state = (state | SYNC_LOAD_FLAG) ^ SYNC_LOAD_FLAG
    }
  }

  /**
   * Returns true if the image should be loaded when first asked for.
   */
  def getLoadsSynchronously: Boolean =
    (state & SYNC_LOAD_FLAG) != 0

  /**
   * Convenience method to get the StyleSheet.
   */
  protected def getStyleSheet: StyleSheet = {
    getDocument.asInstanceOf[HTMLDocument].getStyleSheet
  }

  /**
   * Fetches the attributes to use when rendering.  This is
   * implemented to multiplex the attributes specified in the
   * model with a StyleSheet.
   */
  override def getAttributes: AttributeSet = {
    sync()
    attr
  }

  /**
   * For images the tooltip text comes from text specified with the
   * <code>ALT</code> attribute. This is overriden to return
   * <code>getAltText</code>.
   *
   * @see JTextComponent#getToolTipText
   */
  override def getToolTipText(x: Float, y: Float, allocation: Shape): String =
    getAltText

  /**
   * Update any cached values that come from attributes.
   */
  protected def setPropertiesFromAttributes(): Unit = {
    val sheet = getStyleSheet
    this.attr = sheet.getViewAttributes(this)

    // Gutters
    borderSize = getIntAttr(HTML.Attribute.BORDER, if (isLink) DEFAULT_BORDER else 0).toShort
    leftInset = (getIntAttr(HTML.Attribute.HSPACE, 0) + borderSize).toShort
    rightInset = leftInset
    topInset = (getIntAttr(HTML.Attribute.VSPACE, 0) + borderSize).toShort
    bottomInset = topInset
    borderColor = getDocument.asInstanceOf[StyledDocument].getForeground(getAttributes)
    val attr = getElement.getAttributes

    // Alignment.
    // PENDING: This needs to be changed to support the CSS versions
    // when conversion from ALIGN to VERTICAL_ALIGN is complete.
    var alignment = attr.getAttribute(HTML.Attribute.ALIGN)
    vAlign = 1.0f
    if (alignment != null) {
      alignment = alignment.toString
      if ("top" == alignment) {
        vAlign = 0f
      } else if ("middle" == alignment) {
        vAlign = .5f
      }
    }
    val anchorAttr = attr.getAttribute(HTML.Tag.A).asInstanceOf[AttributeSet]
    if (anchorAttr != null && anchorAttr.isDefined(HTML.Attribute.HREF)) {
      this synchronized {
        state |= LINK_FLAG
      }
    } else
      this.synchronized {
        state = (state | LINK_FLAG) ^ LINK_FLAG
      }
  }

  /**
   * Establishes the parent view for this view.
   * Seize this moment to cache the AWT Container I'm in.
   */
  override def setParent(parent: View): Unit = {
    val oldParent = getParent
    super.setParent(parent)
    container = if (parent != null) getContainer else null
    if (oldParent ne parent) this.synchronized {
      state |= RELOAD_FLAG
    }
  }

  /**
   * Invoked when the Elements attributes have changed. Recreates the image.
   */
  override def changedUpdate(e: DocumentEvent, a: Shape, f: ViewFactory): Unit = {
    super.changedUpdate(e, a, f)
    this synchronized {
      state |= RELOAD_FLAG | RELOAD_IMAGE_FLAG
    }

    // Assume the worst.
    preferenceChanged(null, true, true)
  }

  /**
   * Paints the View.
   *
   * @param g the rendering surface to use
   * @param a the allocated region to render into
   * @see View#paint
   */
  override def paint(g: Graphics, a: Shape): Unit = {
    sync()
    val rect = a match {
      case a: Rectangle => a
      case _            => a.getBounds
    }
    val clip = g.getClipBounds
    fBounds.setBounds(rect)
    paintHighlights(g, a)
    paintBorder(g, rect)
    if (clip != null)
      g.clipRect(
        rect.x + leftInset,
        rect.y + topInset,
        rect.width - leftInset - rightInset,
        rect.height - topInset - bottomInset)
    val host = getContainer
    val img  = getImage(host == null || host.isEnabled)
    if (img != null) {
      if (!hasPixels(img)) {
        // No pixels yet, use the default
        val icon = getLoadingImageIcon
        if (icon != null) icon.paintIcon(host, g, rect.x + leftInset, rect.y + topInset)
      } else { // Draw the image
        g.drawImage(img, rect.x + leftInset, rect.y + topInset, width, height, imageObserver)
      }
    } else {
      val icon = getNoImageIcon
      if (icon != null) icon.paintIcon(host, g, rect.x + leftInset, rect.y + topInset)
      val view = getAltView
      // Paint the view representing the alt text, if its non-null
      if (view != null && ((state & WIDTH_FLAG) == 0 || width > DEFAULT_WIDTH)) { // Assume layout along the y direction
        val altRect = new Rectangle(
          rect.x + leftInset + DEFAULT_WIDTH,
          rect.y + topInset,
          rect.width - leftInset - rightInset - DEFAULT_WIDTH,
          rect.height - topInset - bottomInset)
        view.paint(g, altRect)
      }
    }
    if (clip != null) {
      // Reset clip.
      g.setClip(clip.x, clip.y, clip.width, clip.height)
    }
  }

  protected def paintHighlights(g: Graphics, shape: Shape): Unit = {
    if (container.isInstanceOf[JTextComponent]) {
      val tc = container.asInstanceOf[JTextComponent]
      val h  = tc.getHighlighter
      if (h.isInstanceOf[LayeredHighlighter]) {
        h.asInstanceOf[LayeredHighlighter].paintLayeredHighlights(g, getStartOffset, getEndOffset, shape, tc, this)
      }
    }
  }

  protected def paintBorder(g: Graphics, rect: Rectangle): Unit = {
    val color = borderColor
    if ((borderSize > 0 || image == null) && color != null) {
      val xOffset = leftInset - borderSize
      val yOffset = topInset - borderSize
      g.setColor(color)
      val n = if (image == null) 1 else borderSize
      for (counter <- 0 until n) {
        g.drawRect(
          rect.x + xOffset + counter,
          rect.y + yOffset + counter,
          rect.width - counter - counter - xOffset - xOffset - 1,
          rect.height - counter - counter - yOffset - yOffset - 1)
      }
    }
  }

  /**
   * Determines the preferred span for this view along an
   * axis.
   *
   * @param axis may be either X_AXIS or Y_AXIS
   * @return the span the view would like to be rendered into;
   *         typically the view is told to render into the span
   *         that is returned, although there is no guarantee;
   *         the parent may choose to resize or break the view
   */
  override def getPreferredSpan(axis: Int): Float = {
    sync()
    // If the attributes specified a width/height, always use it!
    if (axis == View.X_AXIS && (state & WIDTH_FLAG) == WIDTH_FLAG) {
      getPreferredSpanFromAltView(axis)
      return width + leftInset + rightInset
    }
    if (axis == View.Y_AXIS && (state & HEIGHT_FLAG) == HEIGHT_FLAG) {
      getPreferredSpanFromAltView(axis)
      return height + topInset + bottomInset
    }
    val image = getImage
    if (image != null) {
      axis match {
        case View.X_AXIS => width + leftInset + rightInset
        case View.Y_AXIS => height + topInset + bottomInset
        case _           => throw new IllegalArgumentException("Invalid axis: " + axis)
      }
    } else {
      val view     = getAltView
      val retValue = if (view != null) view.getPreferredSpan(axis) else 0f
      axis match {
        case View.X_AXIS => retValue + (width + leftInset + rightInset).toFloat
        case View.Y_AXIS => retValue + (height + topInset + bottomInset).toFloat
        case _           => throw new IllegalArgumentException("Invalid axis: " + axis)
      }
    }
  }

  /**
   * Determines the desired alignment for this view along an
   * axis.  This is implemented to give the alignment to the
   * bottom of the icon along the y axis, and the default
   * along the x axis.
   *
   * @param axis may be either X_AXIS or Y_AXIS
   * @return the desired alignment; this should be a value
   *         between 0.0 and 1.0 where 0 indicates alignment at the
   *         origin and 1.0 indicates alignment to the full span
   *         away from the origin; an alignment of 0.5 would be the
   *         center of the view
   */
  override def getAlignment(axis: Int): Float = axis match {
    case View.Y_AXIS => vAlign
    case _           => super.getAlignment(axis)
  }

  /**
   * Provides a mapping from the document model coordinate space
   * to the coordinate space of the view mapped to it.
   *
   * @param pos the position to convert
   * @param a   the allocated region to render into
   * @return the bounding box of the given position
   * @exception BadLocationException  if the given position does not represent a
   *            valid location in the associated document
   * @see View#modelToView
   */
  @throws[BadLocationException]
  override def modelToView(pos: Int, a: Shape, b: Position.Bias): Shape = {
    val p0 = getStartOffset
    val p1 = getEndOffset
    if ((pos >= p0) && (pos <= p1)) {
      val r = a.getBounds
      if (pos == p1) {
        r.x += r.width
      }
      r.width = 0
      return r
    }
    null
  }

  /**
   * Provides a mapping from the view coordinate space to the logical
   * coordinate space of the model.
   *
   * @param x the X coordinate
   * @param y the Y coordinate
   * @param a the allocated region to render into
   * @return the location within the model that best represents the
   *         given point of view
   * @see View#viewToModel
   */
  override def viewToModel(x: Float, y: Float, a: Shape, bias: Array[Position.Bias]): Int = {
    val alloc = a.asInstanceOf[Rectangle]
    if (x < alloc.x + alloc.width) {
      bias(0) = Position.Bias.Forward
      getStartOffset
    } else {
      bias(0) = Position.Bias.Backward
      getEndOffset
    }
  }

  /**
   * Sets the size of the view.  This should cause
   * layout of the view if it has any layout duties.
   *
   * @param width  the width &gt;= 0
   * @param height the height &gt;= 0
   */
  override def setSize(width: Float, height: Float): Unit = {
    sync()
    if (getImage == null) {
      val view = getAltView
      if (view != null) {
        view.setSize(
          Math.max(0f, width - (DEFAULT_WIDTH + leftInset + rightInset).toFloat),
          Math.max(0f, height - (topInset + bottomInset).toFloat))
      }
    }
  }

  /**
   * Returns true if this image within a link?
   */
  protected def isLink =
    (state & LINK_FLAG) == LINK_FLAG

  /**
   * Returns true if the passed in image has a non-zero width and height.
   */
  protected def hasPixels(image: Image) =
    image != null && (image.getHeight(imageObserver) > 0) && (image.getWidth(imageObserver) > 0)

  /**
   * Returns the preferred span of the View used to display the alt text,
   * or 0 if the view does not exist.
   */
  protected def getPreferredSpanFromAltView(axis: Int): Float = {
    if (getImage == null) {
      val view = getAltView
      if (view != null) return view.getPreferredSpan(axis)
    }
    0f
  }

  /**
   * Request that this view be repainted.
   * Assumes the view is still at its last-drawn location.
   */
  protected def repaint(delay: Long): Unit = {
    if (container != null && fBounds != null)
      container.repaint(delay, fBounds.x, fBounds.y, fBounds.width, fBounds.height)
  }

  /**
   * Convenience method for getting an integer attribute from the elements
   * AttributeSet.
   */
  protected def getIntAttr(name: HTML.Attribute, deflt: Int) = {
    val attr = getElement.getAttributes
    if (attr.isDefined(name)) {
      // does not check parents!
      var i     = 0
      val value = attr.getAttribute(name).asInstanceOf[String]
      if (value == null) {
        i = deflt
      } else {
        try {
          i = Math.max(0, value.toInt)
        } catch {
          case _: NumberFormatException => i = deflt
        }
      }
      i
    } else {
      deflt
    }
  }

  /**
   * Makes sure the necessary properties and image is loaded.
   */
  protected def sync(): Unit = {
    var s = state
    if ((s & RELOAD_IMAGE_FLAG) != 0) {
      refreshImage()
    }
    s = state
    if ((s & RELOAD_FLAG) != 0) {
      this.synchronized {
        state = (state | RELOAD_FLAG) ^ RELOAD_FLAG
      }
      setPropertiesFromAttributes()
    }
  }

  /**
   * Loads the image and updates the size accordingly. This should be
   * invoked instead of invoking <code>loadImage</code> or
   * <code>updateImageSize</code> directly.
   */
  protected def refreshImage(): Unit = {
    this.synchronized { // clear out width/height/realoadimage flag and set loading flag
      state = (state | LOADING_FLAG | RELOAD_IMAGE_FLAG | WIDTH_FLAG | HEIGHT_FLAG) ^ (WIDTH_FLAG | HEIGHT_FLAG | RELOAD_IMAGE_FLAG)
    }
    image = null
    width = 0
    height = 0

    try {
      // Load the image
      loadImage()
      // And update the size params
      updateImageSize()
    } finally {
      this.synchronized {
        // Clear out state in case someone threw an exception.
        state = (state | LOADING_FLAG) ^ LOADING_FLAG
      }
    }
  }

  /**
   * Loads the image from the URL <code>getImageURL</code>. This should
   * only be invoked from <code>refreshImage</code>.
   */
  protected def loadImage(): Unit = {
    val src:      URL   = getImageURL
    var newImage: Image = null
    if (src != null) {
      val cache = getDocument.getProperty(IMAGE_CACHE_PROPERTY).asInstanceOf[Dictionary[_, _]]
      if (cache != null) newImage = cache.get(src).asInstanceOf[Image]
      else {
        newImage = createImageFromUrl(src)
        if (newImage != null && getLoadsSynchronously) {
          // Force the image to be loaded by using an ImageIcon.
          val ii = new ImageIcon
          ii.setImage(newImage)
        }
      }
    }
    image = newImage
  }

  protected def createImageFromUrl(src: URL): Image = {
    toolkit.createImage(src)
  }

  /**
   * Recreates and reloads the image.  This should
   * only be invoked from <code>refreshImage</code>.
   */
  protected def updateImageSize(): Unit = {
    var newWidth  = 0
    var newHeight = 0
    var newState  = 0
    val newImage  = getImage
    if (newImage != null) {
      val elem = getElement
      val attr = elem.getAttributes
      // Get the width/height and set the state ivar before calling
      // anything that might cause the image to be loaded, and thus the
      // ImageHandler to be called.
      newWidth = getIntAttr(HTML.Attribute.WIDTH, -1)
      if (newWidth > 0) newState |= WIDTH_FLAG
      newHeight = getIntAttr(HTML.Attribute.HEIGHT, -1)
      if (newHeight > 0) newState |= HEIGHT_FLAG
      if (newWidth <= 0) {
        newWidth = newImage.getWidth(imageObserver)
        if (newWidth <= 0) newWidth = DEFAULT_WIDTH
      }
      if (newHeight <= 0) {
        newHeight = newImage.getHeight(imageObserver)
        if (newHeight <= 0) newHeight = DEFAULT_HEIGHT
      }
      // Make sure the image starts loading:
      if ((newState & (WIDTH_FLAG | HEIGHT_FLAG)) != 0) {
        toolkit.prepareImage(newImage, newWidth, newHeight, imageObserver)
      } else {
        toolkit.prepareImage(newImage, -1, -1, imageObserver)
      }
      var createText = false
      this.synchronized {
        // If imageloading failed, other thread may have called
        // ImageLoader which will null out image, hence we check
        // for it.
        if (image != null) {
          if ((newState & WIDTH_FLAG) == WIDTH_FLAG || width == 0) width = newWidth
          if ((newState & HEIGHT_FLAG) == HEIGHT_FLAG || height == 0) height = newHeight
        } else {
          createText = true
          if ((newState & WIDTH_FLAG) == WIDTH_FLAG) width = newWidth
          if ((newState & HEIGHT_FLAG) == HEIGHT_FLAG) height = newHeight
        }
        state = state | newState
        state = (state | LOADING_FLAG) ^ LOADING_FLAG
      }
      if (createText) { // Only reset if this thread determined image is null
        updateAltTextView()
      }
    } else {
      width = DEFAULT_WIDTH
      height = DEFAULT_HEIGHT
      updateAltTextView()
    }
  }

  /**
   * Updates the view representing the alt text.
   */
  protected def updateAltTextView(): Unit = {
    val text = getAltText
    if (text != null) {
      val newView = new ImageLabelView(getElement, text)
      this.synchronized {
        altView = newView
      }
    }
  }

  /**
   * Returns the view to use for alternate text. This may be null.
   */
  protected def getAltView: View = {
    val view = this.synchronized { altView }
    if (view != null && view.getParent == null) view.setParent(getParent)
    view
  }

  /**
   * Invokes <code>preferenceChanged</code> on the event displatching
   * thread.
   */
  protected def safePreferenceChanged(): Unit = {
    if (SwingUtilities.isEventDispatchThread) {
      val doc = getDocument
      if (doc.isInstanceOf[AbstractDocument]) doc.asInstanceOf[AbstractDocument].readLock()
      preferenceChanged(null, true, true)
      if (doc.isInstanceOf[AbstractDocument]) doc.asInstanceOf[AbstractDocument].readUnlock()
    } else {
      SwingUtilities.invokeLater(() => {
        safePreferenceChanged()
      })
    }
  }

  /**
   * ImageHandler implements the ImageObserver to correctly update the
   * display as new parts of the image become available.
   */
  class ImageHandler extends ImageObserver {
    import java.awt.image.ImageObserver._

    // This can come on any thread. If we are in the process of reloading
    // the image and determining our state (loading == true) we don't fire
    // preference changed, or repaint, we just reset the fWidth/fHeight as
    // necessary and return. This is ok as we know when loading finishes
    // it will pick up the new height/width, if necessary.
    override def imageUpdate(img: Image, flags: Int, x: Int, y: Int, newWidth: Int, newHeight: Int): Boolean = {
      if ((img ne image) && (img ne disabledImage) || image == null || getParent == null) return false
      // Bail out if there was an error:
      if ((flags & (ABORT | ERROR)) != 0) {
        repaint(0)
        BaseImageView.this.synchronized {
          if (image eq img) { // Be sure image hasn't changed since we don't
            // initialy synchronize
            image = null
            if ((state & WIDTH_FLAG) != WIDTH_FLAG) width = DEFAULT_WIDTH
            if ((state & HEIGHT_FLAG) != HEIGHT_FLAG) height = DEFAULT_HEIGHT
          } else disabledImage = null
          if ((state & LOADING_FLAG) == LOADING_FLAG) { // No need to resize or repaint, still in the process
            // of loading.
            return false
          }
        }

        updateAltTextView()
        safePreferenceChanged()
        return false
      }
      if (image eq img) {
        // Resize image if necessary:
        var changed = 0
        if ((flags & ImageObserver.HEIGHT) != 0 && !getElement.getAttributes.isDefined(HTML.Attribute.HEIGHT))
          changed |= 1
        if ((flags & ImageObserver.WIDTH) != 0 && !getElement.getAttributes.isDefined(HTML.Attribute.WIDTH))
          changed |= 2
        BaseImageView.this.synchronized {
          if ((changed & 1) == 1 && (state & WIDTH_FLAG) == 0) width = newWidth
          if ((changed & 2) == 2 && (state & HEIGHT_FLAG) == 0) height = newHeight
          if ((state & LOADING_FLAG) == LOADING_FLAG) {
            // No need to resize or repaint, still in the process of loading.
            return true
          }
        }

        if (changed != 0) { // May need to resize myself, asynchronously:
          safePreferenceChanged()
          return true
        }
      }
      // Repaint when done or when new pixels arrive:
      if ((flags & (FRAMEBITS | ALLBITS)) != 0) repaint(0)
      else if ((flags & SOMEBITS) != 0 && sIsInc) repaint(sIncRate)
      (flags & ALLBITS) == 0
    }
  }

  /**
   * ImageLabelView is used if the image can't be loaded, and
   * the attribute specified an alt attribute. It overriden a handle of
   * methods as the text is hardcoded and does not come from the document.
   */
  class ImageLabelView(val e: Element, val text: String) extends InlineView(e) {
    var segment: Segment = _
    var fg:      Color   = _

    reset(text)

    def reset(text: String): Unit = {
      segment = new Segment(text.toCharArray, 0, text.length)
    }

    override def paint(g: Graphics, a: Shape): Unit = {
      // Don't use supers paint, otherwise selection will be wrong
      // as our start/end offsets are fake.
      val painter = getGlyphPainter
      if (painter != null) {
        g.setColor(getForeground)
        painter.paint(this, g, a, getStartOffset, getEndOffset)
      }
    }

    override def getText(p0: Int, p1: Int): Segment = {
      if (p0 < 0 || p1 > segment.array.length) {
        throw new RuntimeException("ImageLabelView: Stale view")
      }
      segment.offset = p0
      segment.count = p1 - p0
      segment
    }

    override def getStartOffset: Int = 0

    override def getEndOffset: Int = segment.array.length

    override def breakView(axis: Int, p0: Int, pos: Float, len: Float): View = {
      // Don't allow a break
      this
    }

    override def getForeground: Color = {
      val parent = getParent
      if (fg == null && parent != null) {
        val doc  = getDocument
        val attr = parent.getAttributes
        if (attr != null && doc.isInstanceOf[StyledDocument]) {
          fg = doc.asInstanceOf[StyledDocument].getForeground(attr)
        }
      }
      fg
    }
  }
}
