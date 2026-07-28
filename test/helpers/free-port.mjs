// test/helpers/free-port.mjs — asks the OS for a free TCP port, so tests
// can start server.mjs without colliding on a fixed port.

import net from "net";

export function getFreePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.unref();
    srv.on("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}
