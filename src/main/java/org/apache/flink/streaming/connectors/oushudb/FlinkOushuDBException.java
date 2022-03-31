package org.apache.flink.streaming.connectors.oushudb;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.FlinkException;

/** Exception used by {@link FlinkOushuDBSink} */
@PublicEvolving
public class FlinkOushuDBException extends FlinkException {
    private static final long serialVersionUID = -1085986719803605287L;

    private final FlinkOushuDBErrorCode errorCode;

    public FlinkOushuDBException(FlinkOushuDBErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public FlinkOushuDBException(FlinkOushuDBErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public FlinkOushuDBErrorCode getErrorCode() {
        return errorCode;
    }
}
