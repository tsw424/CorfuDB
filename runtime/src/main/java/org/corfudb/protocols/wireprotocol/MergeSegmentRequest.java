package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;

import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.corfudb.runtime.view.Layout.ReplicationMode;

@Data
@AllArgsConstructor
public class MergeSegmentRequest implements ICorfuPayload<MergeSegmentRequest> {

    private LayoutSegment segment;

    public MergeSegmentRequest(ByteBuf buf) {
        ReplicationMode replicationMode = ReplicationMode
                .valueOf(ICorfuPayload.fromBuffer(buf, String.class));
        long start = ICorfuPayload.fromBuffer(buf, Long.class);
        long end = ICorfuPayload.fromBuffer(buf, Long.class);
        int stripeSize = ICorfuPayload.fromBuffer(buf, Integer.class);
        List<LayoutStripe> stripes = new ArrayList<>();
        for (int i = 0; i < stripeSize; i++) {
            stripes.add(new LayoutStripe(ICorfuPayload.listFromBuffer(buf, String.class)));
        }
        segment = new LayoutSegment(replicationMode, start, end, stripes);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, segment.getReplicationMode().toString());
        ICorfuPayload.serialize(buf, segment.getStart());
        ICorfuPayload.serialize(buf, segment.getEnd());
        ICorfuPayload.serialize(buf, segment.getStripes().size());
        segment.getStripes().forEach(layoutStripe ->
                ICorfuPayload.serialize(buf, layoutStripe.getLogServers()));
    }
}
