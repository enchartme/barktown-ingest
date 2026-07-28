// test/helpers/static-file-server.mjs — minimal static file server used as
// a stand-in for the public S3 asset bucket (ASSET_BASE) in tests.

import http from "http";
import fs from "fs";
import path from "path";
import { getFreePort } from "./free-port.mjs";

export async function startStaticServer(rootDir) {
  const root = path.resolve(rootDir);

  const server = http.createServer((req, res) => {
    const reqPath = decodeURIComponent(new URL(req.url, "http://x").pathname);
    const filePath = path.join(root, reqPath);
    if (!filePath.startsWith(root)) {
      res.writeHead(403);
      res.end();
      return;
    }
    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(404);
        res.end();
        return;
      }
      res.writeHead(200, { "Content-Type": "application/octet-stream" });
      res.end(data);
    });
  });

  const port = await getFreePort();
  await new Promise((resolve) => server.listen(port, "127.0.0.1", resolve));

  return {
    baseUrl: `http://127.0.0.1:${port}`,
    stop: () => new Promise((resolve) => server.close(resolve)),
  };
}
