---
title: HTTP Server
description: Building an HTTP/1.1 server from raw TCP — parsing requests, routing, serving files, and returning JSON.
---

An HTTP server is the natural next step after [Echo Server](/cookbook/echo-server). Instead of echoing raw bytes, you parse a structured protocol, route requests to different handlers, and construct well-formed responses. This recipe builds a minimal but correct HTTP/1.1 server entirely from `TcpListener` and `TcpStream` -- no HTTP library required.

:::tip[What you will learn]
- **HTTP request parsing** -- extracting method, path, and headers from raw TCP bytes
- **Path-based routing** -- dispatching requests to different handlers
- **Response construction** -- building status lines, headers, and bodies by hand
- **Static file serving** -- reading files from disk with `fs.readFile` and setting Content-Type
- **Per-connection concurrency** -- spawning a task per connection (building on Echo Server)
:::

## Complete Example

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    try volt.run(server);
}

fn server(io: volt.Runtime) void {
    var listener = volt.net.TcpListener.bind(
        volt.net.Address.fromPort(8080),
    ) catch |err| {
        std.debug.print("bind failed: {}\n", .{err});
        return;
    };
    defer listener.close();

    const port = listener.localAddr().port();
    std.debug.print("HTTP server listening on http://0.0.0.0:{}\n", .{port});
    std.debug.print("Try: curl http://localhost:{}/health\n\n", .{port});

    while (true) {
        if (listener.tryAccept() catch null) |result| {
            var f = io.@"async"(handleConnection, .{result.stream}) catch {
                handleConnection(result.stream);
                continue;
            };
            f.detach();
        } else {
            std.Thread.sleep(1 * std.time.ns_per_ms);
        }
    }
}

// -- HTTP types ---------------------------------------------------------------

const Method = enum { GET, POST, OTHER };

const Request = struct {
    method: Method,
    path: []const u8,
    headers_end: usize,      // byte offset where headers end (after \r\n\r\n)
    content_length: usize,   // from Content-Length header, 0 if absent
    raw: []const u8,         // full raw request bytes

    pub fn body(self: *const Request) []const u8 {
        if (self.headers_end >= self.raw.len) return &.{};
        return self.raw[self.headers_end..];
    }
};

// -- Connection handler -------------------------------------------------------

fn handleConnection(conn: volt.net.TcpStream) void {
    var stream = conn;
    defer stream.close();

    var buf: [8192]u8 = undefined;
    var total: usize = 0;

    // Read until we have a complete set of headers (\r\n\r\n).
    // HTTP/1.1 requires the headers to fit in a single read for simple
    // servers; production servers use a streaming parser instead.
    while (total < buf.len) {
        const n = stream.tryRead(buf[total..]) catch return orelse {
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        };
        if (n == 0) return; // Client disconnected before sending a request.
        total += n;

        // Look for the end-of-headers marker.
        if (std.mem.indexOf(u8, buf[0..total], "\r\n\r\n")) |_| break;
    }

    const raw = buf[0..total];

    // Parse the request line and headers.
    const request = parseRequest(raw) orelse {
        sendResponse(stream, 400, "text/plain", "Bad Request\n");
        return;
    };

    // Route to the appropriate handler.
    route(stream, &request);
}

// -- Request parsing ----------------------------------------------------------

fn parseRequest(raw: []const u8) ?Request {
    // Request line: "GET /path HTTP/1.1\r\n"
    const line_end = std.mem.indexOf(u8, raw, "\r\n") orelse return null;
    const request_line = raw[0..line_end];

    // Split into method, path, version.
    var parts = std.mem.splitScalar(u8, request_line, ' ');
    const method_str = parts.next() orelse return null;
    const path = parts.next() orelse return null;
    // We ignore the HTTP version for simplicity.

    const method: Method = if (std.mem.eql(u8, method_str, "GET"))
        .GET
    else if (std.mem.eql(u8, method_str, "POST"))
        .POST
    else
        .OTHER;

    // Find the end of headers.
    const headers_marker = std.mem.indexOf(u8, raw, "\r\n\r\n") orelse return null;
    const headers_end = headers_marker + 4;

    // Extract Content-Length if present.
    var content_length: usize = 0;
    const headers_section = raw[line_end + 2 .. headers_marker];
    var header_iter = std.mem.splitSequence(u8, headers_section, "\r\n");
    while (header_iter.next()) |header_line| {
        if (std.ascii.startsWithIgnoreCase(header_line, "content-length:")) {
            const value = std.mem.trimLeft(u8, header_line["content-length:".len..], " ");
            content_length = std.fmt.parseInt(usize, value, 10) catch 0;
        }
    }

    return Request{
        .method = method,
        .path = path,
        .headers_end = headers_end,
        .content_length = content_length,
        .raw = raw,
    };
}

// -- Routing ------------------------------------------------------------------

fn route(stream: volt.net.TcpStream, request: *const Request) void {
    if (request.method == .GET and std.mem.eql(u8, request.path, "/")) {
        sendResponse(stream, 200, "text/html", indexPage());
    } else if (request.method == .GET and std.mem.eql(u8, request.path, "/health")) {
        sendResponse(stream, 200, "application/json", "{\"status\":\"ok\"}\n");
    } else if (request.method == .GET and std.mem.eql(u8, request.path, "/api/data")) {
        sendResponse(stream, 200, "application/json",
            \\{"items":[{"id":1,"name":"alpha"},{"id":2,"name":"beta"}]}
            \\
        );
    } else if (request.method == .POST and std.mem.eql(u8, request.path, "/api/echo")) {
        // Echo the request body back as JSON.
        handleEcho(stream, request);
    } else if (request.method == .GET and std.mem.startsWith(u8, request.path, "/static/")) {
        serveStaticFile(stream, request.path["/static/".len..]);
    } else {
        sendResponse(stream, 404, "text/plain", "Not Found\n");
    }
}

fn indexPage() []const u8 {
    return
        \\<!DOCTYPE html>
        \\<html>
        \\<head><title>Volt HTTP Server</title></head>
        \\<body>
        \\  <h1>Volt HTTP Server</h1>
        \\  <ul>
        \\    <li><a href="/health">Health Check</a></li>
        \\    <li><a href="/api/data">API Data</a></li>
        \\  </ul>
        \\</body>
        \\</html>
        \\
    ;
}

// -- Response helpers ---------------------------------------------------------

fn sendResponse(
    stream: volt.net.TcpStream,
    status: u16,
    content_type: []const u8,
    body_content: []const u8,
) void {
    var conn = stream;
    var header_buf: [512]u8 = undefined;

    const status_text = switch (status) {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        else => "Unknown",
    };

    const header = std.fmt.bufPrint(&header_buf,
        "HTTP/1.1 {d} {s}\r\n" ++
        "Content-Type: {s}\r\n" ++
        "Content-Length: {d}\r\n" ++
        "Connection: close\r\n" ++
        "\r\n",
        .{ status, status_text, content_type, body_content.len },
    ) catch return;

    conn.writeAll(header) catch return;
    conn.writeAll(body_content) catch return;
}

fn handleEcho(stream: volt.net.TcpStream, request: *const Request) void {
    const body_data = request.body();

    // Wrap the echoed body in a JSON envelope.
    var response_buf: [4096]u8 = undefined;
    const response = std.fmt.bufPrint(&response_buf,
        "{{\"echoed\":\"{s}\",\"length\":{d}}}",
        .{ body_data, body_data.len },
    ) catch {
        sendResponse(stream, 500, "text/plain", "Response too large\n");
        return;
    };

    sendResponse(stream, 200, "application/json", response);
}

fn serveStaticFile(stream: volt.net.TcpStream, filename: []const u8) void {
    // Prevent path traversal attacks: reject any path containing "..".
    if (std.mem.indexOf(u8, filename, "..") != null) {
        sendResponse(stream, 400, "text/plain", "Invalid path\n");
        return;
    }

    // Build the full path relative to a "public/" directory.
    var path_buf: [256]u8 = undefined;
    const path = std.fmt.bufPrint(&path_buf, "public/{s}", .{filename}) catch {
        sendResponse(stream, 400, "text/plain", "Path too long\n");
        return;
    };

    // Read the file contents. In a production server you would use
    // io.concurrent here since disk I/O can block.
    var file_buf: [65536]u8 = undefined;
    const file = volt.fs.File.open(path) catch {
        sendResponse(stream, 404, "text/plain", "File not found\n");
        return;
    };
    defer file.close();

    const n = file.read(&file_buf) catch {
        sendResponse(stream, 500, "text/plain", "Read error\n");
        return;
    };

    const content_type = guessContentType(filename);
    sendResponse(stream, 200, content_type, file_buf[0..n]);
}

fn guessContentType(filename: []const u8) []const u8 {
    if (std.mem.endsWith(u8, filename, ".html")) return "text/html";
    if (std.mem.endsWith(u8, filename, ".css")) return "text/css";
    if (std.mem.endsWith(u8, filename, ".js")) return "application/javascript";
    if (std.mem.endsWith(u8, filename, ".json")) return "application/json";
    if (std.mem.endsWith(u8, filename, ".png")) return "image/png";
    if (std.mem.endsWith(u8, filename, ".jpg")) return "image/jpeg";
    if (std.mem.endsWith(u8, filename, ".svg")) return "image/svg+xml";
    if (std.mem.endsWith(u8, filename, ".txt")) return "text/plain";
    return "application/octet-stream";
}
```

## Walkthrough

### Why parse HTTP by hand?

HTTP/1.1 is a text protocol simple enough to parse with `std.mem` operations. Building the parser yourself teaches you exactly how request framing works -- where the method ends, where headers begin, and how `Content-Length` determines how many body bytes to read. When you later adopt an HTTP library, you will understand what it is doing under the hood.

The parser here is deliberately minimal. It handles single-request connections (`Connection: close`). A production parser would handle chunked transfer encoding, pipelining, and malformed input more defensively.

### Request parsing strategy

The parser reads bytes into a fixed buffer until it sees `\r\n\r\n` -- the boundary between headers and body in HTTP/1.1. Once found:

1. The **request line** (`GET /path HTTP/1.1`) is extracted from the first `\r\n`.
2. The method and path are split on spaces.
3. Headers are scanned for `Content-Length` to know how much body to expect.

This approach works because HTTP headers are required to be ASCII and reasonably short. The 8 KiB buffer handles typical requests; production servers allocate larger buffers or stream headers.

### Routing with pattern matching

The `route` function is a simple chain of `if`/`else if` checks. For a small number of endpoints this is clear and efficient. As a server grows beyond 10-15 routes, you would switch to a trie-based router or a hash map from path to handler function pointer.

The `startsWith` check for `/static/` demonstrates prefix routing -- any path under `/static/` is served from disk. The path traversal guard (`..` rejection) is essential to prevent clients from reading arbitrary files outside the public directory.

### Static file serving

Files are read synchronously with `volt.fs.File.open` and `file.read`. For a small number of concurrent requests this works fine. Under heavy load, disk reads can block the I/O worker. The [Work Offloading](/cookbook/work-offloading) recipe shows how to move file reads to the blocking pool with `io.concurrent`.

Content-Type is guessed from the file extension. A production server would also send `Last-Modified`, `ETag`, and `Cache-Control` headers for caching.

### Response construction

Every response follows the same structure:

```
HTTP/1.1 {status} {reason}\r\n
Content-Type: {type}\r\n
Content-Length: {len}\r\n
Connection: close\r\n
\r\n
{body}
```

`Content-Length` is set from the body length *before* writing, so the client knows exactly how many bytes to read. `Connection: close` tells the client we will close the socket after the response, simplifying our implementation (no keep-alive state tracking).

## Variations

### Request logging middleware

Add latency tracking to every request by wrapping the route dispatch with timing:

```zig
const std = @import("std");
const volt = @import("volt");

fn handleConnection(conn: volt.net.TcpStream) void {
    var stream = conn;
    defer stream.close();

    var buf: [8192]u8 = undefined;
    var total: usize = 0;

    while (total < buf.len) {
        const n = stream.tryRead(buf[total..]) catch return orelse {
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        };
        if (n == 0) return;
        total += n;
        if (std.mem.indexOf(u8, buf[0..total], "\r\n\r\n")) |_| break;
    }

    const request = parseRequest(buf[0..total]) orelse {
        sendResponse(stream, 400, "text/plain", "Bad Request\n");
        return;
    };

    // Log: method, path, status, latency.
    const start = volt.Instant.now();
    route(stream, &request);
    const elapsed = start.elapsed();

    const method_str: []const u8 = switch (request.method) {
        .GET => "GET",
        .POST => "POST",
        .OTHER => "???",
    };

    std.debug.print("{s} {s} -- {d}ms\n", .{
        method_str,
        request.path,
        elapsed.asMillis(),
    });
}
```

This is the foundation for access logging. In a production server you would also log the response status code, client IP, and request ID.

### POST body handling

Handle POST requests that send JSON or form data by reading the full body based on `Content-Length`:

```zig
fn handlePost(stream: volt.net.TcpStream, request: *const Request) void {
    if (request.content_length == 0) {
        sendResponse(stream, 400, "text/plain", "Missing request body\n");
        return;
    }

    // The body may already be partially (or fully) in the initial read
    // buffer. Read any remaining bytes.
    const already_read = request.body();
    var body_buf: [4096]u8 = undefined;

    if (already_read.len > body_buf.len) {
        sendResponse(stream, 413, "text/plain", "Body too large\n");
        return;
    }

    @memcpy(body_buf[0..already_read.len], already_read);
    var body_len = already_read.len;

    var conn = stream;
    while (body_len < request.content_length and body_len < body_buf.len) {
        const n = conn.tryRead(body_buf[body_len..]) catch {
            sendResponse(stream, 500, "text/plain", "Read error\n");
            return;
        } orelse {
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        };
        if (n == 0) break;
        body_len += n;
    }

    // Echo the body back wrapped in JSON.
    var response_buf: [8192]u8 = undefined;
    const response = std.fmt.bufPrint(&response_buf,
        "{{\"received_bytes\":{d},\"body\":\"{s}\"}}",
        .{ body_len, body_buf[0..body_len] },
    ) catch {
        sendResponse(stream, 500, "text/plain", "Response overflow\n");
        return;
    };

    sendResponse(stream, 200, "application/json", response);
}
```

Test it with: `curl -X POST -d '{"key":"value"}' http://localhost:8080/api/echo`

### Keep-alive connections

Handle multiple requests per connection by looping instead of closing after one response. This improves throughput because it avoids the overhead of TCP handshakes for sequential requests from the same client.

```zig
fn handleKeepAlive(conn: volt.net.TcpStream) void {
    var stream = conn;
    defer stream.close();

    var request_count: u32 = 0;
    const max_requests: u32 = 100; // Limit per connection to prevent abuse.

    while (request_count < max_requests) {
        var buf: [8192]u8 = undefined;
        var total: usize = 0;

        // Read next request on this connection.
        while (total < buf.len) {
            const n = stream.tryRead(buf[total..]) catch return orelse {
                // Use a longer timeout between requests to detect idle
                // connections. 30 seconds is a common keep-alive timeout.
                std.Thread.sleep(10 * std.time.ns_per_ms);
                continue;
            };
            if (n == 0) return; // Client closed the connection.
            total += n;
            if (std.mem.indexOf(u8, buf[0..total], "\r\n\r\n")) |_| break;
        }

        const request = parseRequest(buf[0..total]) orelse {
            sendKeepAliveResponse(stream, 400, "text/plain", "Bad Request\n", false);
            return;
        };

        request_count += 1;
        const keep_going = request_count < max_requests;

        // Route the request, sending Connection: keep-alive or close.
        routeKeepAlive(stream, &request, keep_going);
    }
}

fn sendKeepAliveResponse(
    stream: volt.net.TcpStream,
    status: u16,
    content_type: []const u8,
    body_content: []const u8,
    keep_alive: bool,
) void {
    var conn = stream;
    var header_buf: [512]u8 = undefined;

    const status_text = switch (status) {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        else => "Unknown",
    };

    const conn_header: []const u8 = if (keep_alive) "keep-alive" else "close";

    const header = std.fmt.bufPrint(&header_buf,
        "HTTP/1.1 {d} {s}\r\n" ++
        "Content-Type: {s}\r\n" ++
        "Content-Length: {d}\r\n" ++
        "Connection: {s}\r\n" ++
        "\r\n",
        .{ status, status_text, content_type, body_content.len, conn_header },
    ) catch return;

    conn.writeAll(header) catch return;
    conn.writeAll(body_content) catch return;
}

fn routeKeepAlive(
    stream: volt.net.TcpStream,
    request: *const Request,
    keep_alive: bool,
) void {
    if (request.method == .GET and std.mem.eql(u8, request.path, "/health")) {
        sendKeepAliveResponse(stream, 200, "application/json",
            "{\"status\":\"ok\"}\n", keep_alive);
    } else {
        sendKeepAliveResponse(stream, 404, "text/plain",
            "Not Found\n", keep_alive);
    }
}
```

Keep-alive is the default in HTTP/1.1. Without it, every request requires a new TCP connection (three-way handshake + slow start), which adds significant latency. The `max_requests` limit prevents a single client from monopolizing a connection indefinitely.
