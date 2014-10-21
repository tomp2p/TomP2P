package net.tomp2p.message;

import net.tomp2p.message.Message.Content;

public class ContentTypeIndex {
    private final int number;
    private final Content content;
    public ContentTypeIndex(int number, Content content) {
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
