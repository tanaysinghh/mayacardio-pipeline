package com.mayacardio.pipeline.tools.harness;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class NetProxy implements AutoCloseable {
    private final ServerSocket server;
    private final String targetHost;
    private final int targetPort;
    private final List<Socket> sockets = new CopyOnWriteArrayList<>();
    private volatile boolean cut;
    private volatile boolean running = true;

    public NetProxy(String targetHost, int targetPort) throws IOException {
        this.server = new ServerSocket(0, 100, InetAddress.getLoopbackAddress());
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        Thread t = new Thread(this::acceptLoop, "netproxy-accept");
        t.setDaemon(true);
        t.start();
    }

    public int port() {
        return server.getLocalPort();
    }

    public boolean isCut() {
        return cut;
    }

    public void cut() {
        cut = true;
    }

    public void restore() {
        for (Socket s : sockets) {
            closeQuietly(s);
        }
        sockets.clear();
        cut = false;
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket client = server.accept();
                sockets.add(client);
                if (cut) {
                    continue;
                }
                Socket upstream;
                try {
                    upstream = new Socket(targetHost, targetPort);
                } catch (IOException e) {
                    closeQuietly(client);
                    continue;
                }
                sockets.add(upstream);
                pump(client, upstream);
                pump(upstream, client);
            } catch (IOException e) {
                if (!running) {
                    return;
                }
            }
        }
    }

    private void pump(Socket from, Socket to) {
        Thread t = new Thread(() -> {
            byte[] buf = new byte[32 * 1024];
            try {
                from.setSoTimeout(200);
                InputStream in = from.getInputStream();
                OutputStream out = to.getOutputStream();
                while (running && !from.isClosed()) {
                    if (cut) {
                        Thread.sleep(50);
                        continue;
                    }
                    int n;
                    try {
                        n = in.read(buf);
                    } catch (SocketTimeoutException e) {
                        continue;
                    }
                    if (n < 0) {
                        break;
                    }
                    while (cut && running) {
                        Thread.sleep(50);
                    }
                    if (from.isClosed() || to.isClosed()) {
                        break;
                    }
                    out.write(buf, 0, n);
                    out.flush();
                }
            } catch (IOException | InterruptedException ignored) {
            } finally {
                closeQuietly(from);
                closeQuietly(to);
            }
        }, "netproxy-pump");
        t.setDaemon(true);
        t.start();
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        server.close();
        for (Socket s : sockets) {
            closeQuietly(s);
        }
    }
}
