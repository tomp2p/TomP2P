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
    private final int number;
    private final Content content;
    public MessageContentIndex(int number, Content content) {
        this.number = number;
        this.content = content;
    }
    
    public int number() {
        return number;
    }
    
    public Content content() {
        return content;
    }
}
