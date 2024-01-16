package org.fs.chm.ui.swing.general;

import javax.swing.text.AttributeSet;
import javax.swing.text.html.Option;

/** Wrapper around {@link Option} to expose its protected method */
public class CustomOption extends Option {
    public CustomOption(AttributeSet attr) {
        super(attr);
    }

    @Override
    public void setSelection(boolean state) {
        super.setSelection(state);
    }
}
