// QCL 聊天室后端（腾讯云 CloudBase 云托管 CloudRun，函数型）
//
// 用「云托管」而非「云函数」的原因：云函数 WebSocket 是"一条连接一个实例"，
// 两个客户端会连到两个互不相通的实例，无法互相广播。云托管是常驻容器，
// 一个实例持有所有在线连接，天然支持多人聊天广播；无连接时缩容到 0，零成本。
//
// 函数型云托管固定监听 3000 端口，平台负责把客户端的 WebSocket 请求路由进来。
//
// 协议（JSON 文本帧，与客户端 QWebSocket 约定一致）：
//   客户端 → 服务器：{type:"join", name:"昵称"}  {type:"chat", text:"..."}  {type:"ping", t:123}
//   服务器 → 客户端：{type:"history", messages:[...]}  {type:"chat", msg:{...}}
//                    {type:"online", count:N}  {type:"pong", t:123}

const WebSocket = require("ws");

const MAX_HISTORY = 200;
const MAX_NAME = 24;
const MAX_TEXT = 500;

const wss = new WebSocket.Server({ port: 3000 });
const sessions = new Map(); // ws -> { name }
let history = []; // 内存历史，最近 200 条（实例冷启动后清空，实时聊天可接受）

function send(ws, obj) {
  try {
    ws.send(JSON.stringify(obj));
  } catch (e) {
    // 忽略已断开的连接
  }
}

function broadcast(obj) {
  const raw = JSON.stringify(obj);
  for (const ws of sessions.keys()) {
    try {
      ws.send(raw);
    } catch (e) {
      // 忽略
    }
  }
}

function broadcastOnline() {
  broadcast({ type: "online", count: sessions.size });
}

wss.on("connection", (ws) => {
  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }
    switch (msg.type) {
      case "join": {
        const name = String(msg.name || "").slice(0, MAX_NAME) || "匿名";
        sessions.set(ws, { name });
        send(ws, { type: "history", messages: history });
        broadcastOnline();
        break;
      }
      case "chat": {
        const text = String(msg.text || "").slice(0, MAX_TEXT);
        if (!text.trim()) return;
        const rec = sessions.get(ws);
        const name = rec ? rec.name : "匿名";
        const item = {
          id: Date.now(),
          name,
          text: text.trim(),
          ts: Math.floor(Date.now() / 1000),
        };
        history.push(item);
        if (history.length > MAX_HISTORY) {
          history.splice(0, history.length - MAX_HISTORY);
        }
        broadcast({ type: "chat", msg: item });
        break;
      }
      case "ping": {
        send(ws, { type: "pong", t: msg.t });
        break;
      }
    }
  });
  ws.on("close", () => {
    sessions.delete(ws);
    broadcastOnline();
  });
  ws.on("error", () => {
    sessions.delete(ws);
    broadcastOnline();
  });
});
