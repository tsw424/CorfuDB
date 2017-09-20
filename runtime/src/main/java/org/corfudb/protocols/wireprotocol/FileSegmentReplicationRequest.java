package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class FileSegmentReplicationRequest implements ICorfuPayload<FileSegmentReplicationRequest> {

    private int fileSegmentIndex;
    private byte[] fileBuffer;

    public FileSegmentReplicationRequest(ByteBuf buf) {
        fileSegmentIndex = ICorfuPayload.fromBuffer(buf, Integer.class);
        fileBuffer = ICorfuPayload.fromBuffer(buf, byte[].class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, fileSegmentIndex);
        ICorfuPayload.serialize(buf, fileBuffer);
    }
}
