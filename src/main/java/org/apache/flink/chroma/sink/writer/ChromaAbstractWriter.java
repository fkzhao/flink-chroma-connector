package org.apache.flink.chroma.sink.writer;

import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;

public interface ChromaAbstractWriter<InputT, WriterStateT, CommT>
        extends StatefulSink.StatefulSinkWriter<InputT, WriterStateT>,
        TwoPhaseCommittingSink.PrecommittingSinkWriter<InputT, CommT>{
}
