package org.example;

public class UpdateEvent<T> {
    public OpType opType;
    public T payload;

    public UpdateEvent() {}

    public UpdateEvent(OpType op, T data) {
        this.opType = op;
        this.payload = data;
    }
}