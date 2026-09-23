/// KiteFile 前端 API 客户端
///
/// 双模式：
/// - 本机守护进程模式：通过相对路径 `/api` 调用本机 Rust 网关（性能等同原生）
/// - 纯浏览器模式：通过 WebRTC 直接连接对端（性能受限，待实现）

export interface Device {
  id: string;
  name: string;
  ip: string;
  gateway_port: number;
  transfer_port: number;
  platform: string;
  last_seen: number;
}

export interface WhoAmI {
  id: string;
  name: string;
  platform: string;
  gateway_port: number;
  transfer_port: number;
}

export type TransferStatus =
  | 'Pending'
  | 'InProgress'
  | 'Completed'
  | 'Failed'
  | 'Canceled';

export interface TransferProgress {
  file_id: string;
  file_name: string;
  file_size: number;
  bytes_transferred: number;
  chunks_done: number;
  chunks_total: number;
  speed_bps: number;
  status: TransferStatus;
  error: string | null;
  /// true = 接收方视角；false = 发送方视角
  incoming?: boolean;
}

/// 接收方待决的传入请求
export interface IncomingEntry {
  incoming_id: string;
  file_id: string;
  file_name: string;
  file_size: number;
  sha256: string | null;
  from_id: string;
  from_name: string;
  from_ip: string;
  from_gateway_port: number;
  from_transfer_port: number;
  created_at: number;
  decision: boolean | null;
}

/// WS 推送事件（后端 WsEvent，tag = event_type，payload 平铺）
export type WsEvent =
  | ({ event_type: 'progress' } & TransferProgress)
  | ({ event_type: 'incoming' } & IncomingEntry)
  | { event_type: 'incoming_resolved'; incoming_id: string; accepted: boolean };

const API_BASE = ''; // 通过 Vite 代理转发到本机 Rust 守护进程

export async function fetchWhoAmI(): Promise<WhoAmI> {
  const r = await fetch(`${API_BASE}/api/whoami`);
  if (!r.ok) throw new Error(`whoami failed: ${r.status}`);
  return r.json();
}

export async function fetchDevices(): Promise<Device[]> {
  const r = await fetch(`${API_BASE}/api/devices`);
  if (!r.ok) throw new Error(`list devices failed: ${r.status}`);
  return r.json();
}

export async function sendFile(
  targetIp: string,
  targetPort: number,
  filePath: string,
  targetGatewayPort?: number
): Promise<{ file_id: string }> {
  const r = await fetch(`${API_BASE}/api/send`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      target_ip: targetIp,
      target_port: targetPort,
      target_gateway_port: targetGatewayPort,
      file_path: filePath,
    }),
  });
  if (!r.ok) throw new Error(`send failed: ${r.status}`);
  return r.json();
}

export async function cancelTransfer(fileId: string): Promise<void> {
  const r = await fetch(
    `${API_BASE}/api/cancel/${encodeURIComponent(fileId)}`,
    { method: 'POST' }
  );
  if (!r.ok && r.status !== 404) throw new Error(`cancel failed: ${r.status}`);
}

export async function listIncoming(): Promise<IncomingEntry[]> {
  const r = await fetch(`${API_BASE}/api/incoming`);
  if (!r.ok) throw new Error(`list incoming failed: ${r.status}`);
  return r.json();
}

export async function acceptIncoming(incomingId: string): Promise<void> {
  const r = await fetch(
    `${API_BASE}/api/incoming/${encodeURIComponent(incomingId)}/accept`,
    { method: 'POST' }
  );
  if (!r.ok) throw new Error(`accept failed: ${r.status}`);
}

export async function rejectIncoming(incomingId: string): Promise<void> {
  const r = await fetch(
    `${API_BASE}/api/incoming/${encodeURIComponent(incomingId)}/reject`,
    { method: 'POST' }
  );
  if (!r.ok) throw new Error(`reject failed: ${r.status}`);
}

export async function listReceivedFiles(): Promise<string[]> {
  const r = await fetch(`${API_BASE}/api/files`);
  if (!r.ok) throw new Error(`list files failed: ${r.status}`);
  return r.json();
}

/// 订阅实时事件（WebSocket），按 event_type 分发
export function subscribeEvents(onMsg: (ev: WsEvent) => void): () => void {
  const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
  // 通过 Vite 代理 ws → 本机 :7878
  const url = `${proto}://${window.location.host}/ws/progress`;
  const ws = new WebSocket(url);
  ws.onmessage = (e) => {
    try {
      const ev = JSON.parse(e.data) as WsEvent;
      onMsg(ev);
    } catch (err) {
      console.error('parse ws message failed', err);
    }
  };
  return () => ws.close();
}

/// 格式化字节为人类可读
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${units[i]}`;
}

/// 格式化速度（字节/秒 → MB/s 等）
export function formatSpeed(bps: number): string {
  return `${formatBytes(bps)}/s`;
}
