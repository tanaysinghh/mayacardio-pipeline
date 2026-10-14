package com.mayacardio.pipeline.cloud;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public final class TokenFilter implements Filter {
    private final byte[] expected;

    public TokenFilter(String token) {
        this.expected = token == null || token.isBlank() ? null : ("Bearer " + token.trim()).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws IOException, ServletException {
        String path = ((HttpServletRequest) req).getRequestURI();
        if (expected != null && path.startsWith("/api/")) {
            String header = ((HttpServletRequest) req).getHeader("Authorization");
            byte[] got = header == null ? new byte[0] : header.getBytes(StandardCharsets.UTF_8);
            if (!MessageDigest.isEqual(expected, got)) {
                HttpServletResponse r = (HttpServletResponse) res;
                r.setStatus(401);
                r.setContentType("application/json");
                r.getWriter().write("{\"error\": \"unauthorized\"}");
                return;
            }
        }
        chain.doFilter(req, res);
    }
}
