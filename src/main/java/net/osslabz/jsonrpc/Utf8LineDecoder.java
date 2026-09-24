package net.osslabz.jsonrpc;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Splits a byte stream into newline-terminated UTF-8 lines.
 *
 * <p>Decodes only complete lines, so a multi-byte character split across two reads stays intact. Not thread-safe.
 */
final class Utf8LineDecoder {

    private final ByteArrayOutputStream incompleteLine = new ByteArrayOutputStream();

    /** Consumes the buffer's remaining bytes and returns the lines they complete, without the newline. */
    List<String> decode(ByteBuffer bytes) {

        List<String> lines = new ArrayList<>();
        while (bytes.hasRemaining()) {
            byte next = bytes.get();
            if (next == '\n') {
                lines.add(incompleteLine.toString(StandardCharsets.UTF_8));
                incompleteLine.reset();
            } else {
                incompleteLine.write(next);
            }
        }
        return lines;
    }

    void reset() {

        incompleteLine.reset();
    }
}
