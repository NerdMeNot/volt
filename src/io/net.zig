//! v1.1 deprecated alias façade — pre-existing names from when TCP
//! types lived under `volt.io.{TcpListener,TcpStream,Address}`. The
//! canonical home is now `volt.net.*` (P2.A folder promotion).
//!
//! These aliases survive for one minor cycle so consumers can rename
//! at their own pace. Removed in v1.2.

const net = @import("../net/net.zig");

pub const Address = net.Address;
pub const TcpListener = net.TcpListener;
pub const TcpStream = net.TcpStream;
pub const ListenError = net.ListenError;
pub const AcceptError = net.AcceptError;
pub const ConnectError = net.ConnectError;

test {
    _ = net;
}
