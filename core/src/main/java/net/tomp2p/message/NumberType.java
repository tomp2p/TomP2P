package net.tomp2p.message;

import net.tomp2p.message.Message2.Content;

public class NumberType {
    private final int number;
    private final Content content;
    public NumberType(int number, Content content) {
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
