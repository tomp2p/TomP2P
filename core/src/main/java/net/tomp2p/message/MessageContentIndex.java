package net.tomp2p.message;

import net.tomp2p.message.Message.Content;

/**
 * Describes the index of a {@code Message.Content} enum in a {@code Message}.
 * <b>Note:</b> Each {@code Message} can contain up to 8 contents, so indices range from 0 to 7.
 * 
 * @author Thomas Bocek
 *
 */
public class MessageContentIndex {
    private final int index;
    private final Content content;
    public MessageContentIndex(int index, Content content) {
        this.index = index;
        this.content = content;
    }
    
    /**
     * The index of the associated content.
     * 
     * @return The index of the associated content.
     */
    public int index() {
        return index;
    }
    
    /**
     * The content of the associated index.
     * 
     * @return The content of the associated index.
     */
    public Content content() {
        return content;
    }
}
