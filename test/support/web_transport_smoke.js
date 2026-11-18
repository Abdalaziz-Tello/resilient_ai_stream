// Usage: node web_transport_smoke.js <compiled.js>
const http = require('http');
globalThis.self = globalThis;
let slowClosed = false;
const enc = new TextEncoder();
const server = http.createServer((req, res) => {
  const sse = { 'Content-Type': 'text/event-stream; charset=utf-8', 'Access-Control-Allow-Origin': '*' };
  if (req.url === '/ok') {
    if (req.headers['x-test'] !== 'yes') { res.writeHead(400); return res.end('missing header'); }
    let body = '';
    req.on('data', (d) => (body += d));
    req.on('end', () => {
      if (!body.includes('مرحبا')) { res.writeHead(400); return res.end('bad body'); }
      res.writeHead(200, sse);
      const frame =
        'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"مرحبا بك 👋"}}\n\n' +
        'data: {"sources":[{"id":"1","title":"Doc"}]}\n\n' +
        'data: {"type":"message_stop"}\n\n';
      const bytes = Buffer.from(frame);
      // Split inside a multi-byte character, several tiny chunks.
      const cut = bytes.indexOf(Buffer.from('بك')) + 1;
      res.write(bytes.subarray(0, cut));
      setTimeout(() => { res.write(bytes.subarray(cut, cut + 7)); res.write(bytes.subarray(cut + 7)); res.end(); }, 20);
    });
  } else if (req.url === '/status500') {
    res.writeHead(500, { 'Access-Control-Allow-Origin': '*' }); res.end('boom');
  } else if (req.url === '/json') {
    res.writeHead(200, { 'Content-Type': 'application/json' }); res.end('{}');
  } else if (req.url === '/slow') {
    res.writeHead(200, sse); res.write('data: {"text":"x"}\n\n');
    res.on('close', () => { slowClosed = true; });
  } else if (req.url === '/responses') {
    res.writeHead(200, sse);
    for (const e of [
      { type: 'response.created', response: { id: 'r1' } },
      { type: 'response.output_text.delta', delta: 'hi ' },
      { type: 'response.output_text.delta', delta: 'there' },
      { type: 'response.completed', response: { usage: { input_tokens: 3, output_tokens: 4 } } },
    ]) res.write('data: ' + JSON.stringify(e) + '\n\n');
    res.end();
  } else { res.writeHead(404); res.end(); }
});
server.listen(0, '127.0.0.1', () => {
  globalThis.__SSE_BASE__ = 'http://127.0.0.1:' + server.address().port;
  require(require('path').resolve(process.argv[2]));
  const t0 = Date.now();
  const poll = setInterval(() => {
    const r = globalThis.__smokeResult;
    if (r === undefined && Date.now() - t0 < 15000) return;
    clearInterval(poll);
    server.closeAllConnections?.(); server.close();
    if (r !== 'ok') { console.error('Web transport smoke failed:', r ?? 'timeout'); process.exit(1); }
    if (!slowClosed) { console.error('cancel did not abort the fetch'); process.exit(1); }
    console.log('Web transport smoke passed (fetch streaming, errors, cancel, universal adapter)');
  }, 50);
});
