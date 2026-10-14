package com.mayacardio.pipeline.device.collector;

import com.mayacardio.pipeline.device.Clocks;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

public final class MockClient implements Closeable {
    private static final int MAX_LINE = 4 * 1024 * 1024;

    public enum Outcome { OK, CORRUPT, TIMEOUT, DISCONNECTED }

    public record Result(Outcome outcome, MockReply reply, String detail) {
    }

    private final String host;
    private final int port;
    private final long timeoutMs;
    private Socket socket;
    private InputStream in;
    private OutputStream out;
    private final ByteArrayOutputStream lineBuf = new ByteArrayOutputStream(16384);

    public MockClient(String host, int port, long timeoutMs) {
        this.host = host;
        this.port = port;
        this.timeoutMs = timeoutMs;
    }

    public boolean connected() {
        return socket != null && !socket.isClosed();
    }

    public void connect() throws IOException {
        close();
        Socket s = new Socket();
        s.setTcpNoDelay(true);
        s.connect(new InetSocketAddress(host, port), (int) Math.max(500, timeoutMs));
        socket = s;
        in = new BufferedInputStream(s.getInputStream(), 65536);
        out = s.getOutputStream();
        lineBuf.reset();
    }

    public Result read(int sensor, Long fromIndex, ReplyFilter filter) {
        if (!connected()) {
            return new Result(Outcome.DISCONNECTED, null, "not connected");
        }
        String req = fromIndex == null
                ? "{\"cmd\": \"read\", \"sensor\": " + sensor + "}\n"
                : "{\"cmd\": \"read\", \"sensor\": " + sensor + ", \"from_index\": " + fromIndex + "}\n";
        try {
            out.write(req.getBytes(StandardCharsets.UTF_8));
            out.flush();
        } catch (IOException e) {
            close();
            return new Result(Outcome.DISCONNECTED, null, e.getMessage());
        }
        long deadline = Clocks.monoMs() + timeoutMs;
        while (true) {
            long remaining = deadline - Clocks.monoMs();
            if (remaining <= 0) {
                return new Result(Outcome.TIMEOUT, null, "no reply within " + timeoutMs + " ms");
            }
            String line;
            try {
                line = readLine((int) remaining);
            } catch (SocketTimeoutException e) {
                return new Result(Outcome.TIMEOUT, null, "no reply within " + timeoutMs + " ms");
            } catch (IOException e) {
                close();
                return new Result(Outcome.DISCONNECTED, null, e.getMessage());
            }
            if (line == null) {
                close();
                return new Result(Outcome.DISCONNECTED, null, "connection closed by mock");
            }
            if (line.isBlank()) {
                continue;
            }
            MockReply reply;
            try {
                reply = ReplyParser.parse(line);
            } catch (ReplyParser.CorruptReplyException e) {
                return new Result(Outcome.CORRUPT, null, e.getMessage());
            }
            if (filter.stale(reply, sensor, fromIndex)) {
                continue;
            }
            return new Result(Outcome.OK, reply, null);
        }
    }

    public interface ReplyFilter {
        boolean stale(MockReply reply, int requestedSensor, Long requestedFrom);
    }

    private String readLine(int timeout) throws IOException {
        socket.setSoTimeout(Math.max(1, timeout));
        while (true) {
            int b = in.read();
            if (b < 0) {
                return null;
            }
            if (b == '\n') {
                String s = lineBuf.toString(StandardCharsets.UTF_8);
                lineBuf.reset();
                return s;
            }
            if (lineBuf.size() >= MAX_LINE) {
                lineBuf.reset();
                throw new IOException("reply line too long");
            }
            lineBuf.write(b);
        }
    }

    @Override
    public void close() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
        socket = null;
        in = null;
        out = null;
        lineBuf.reset();
    }
}
