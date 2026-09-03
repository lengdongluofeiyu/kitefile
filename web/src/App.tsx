import { useEffect, useState, useCallback } from 'react';
import {
  fetchWhoAmI,
  fetchDevices,
  sendFile,
  cancelTransfer,
  listReceivedFiles,
  listIncoming,
  acceptIncoming,
  rejectIncoming,
  subscribeEvents,
  formatBytes,
  formatSpeed,
  type Device,
  type WhoAmI,
  type TransferProgress,
  type IncomingEntry,
} from './api';

export default function App() {
  const [me, setMe] = useState<WhoAmI | null>(null);
  const [devices, setDevices] = useState<Device[]>([]);
  const [progress, setProgress] = useState<Record<string, TransferProgress>>({});
  /// 待决定的传入请求（incoming_id → entry）
  const [incoming, setIncoming] = useState<Record<string, IncomingEntry>>({});
  const [receivedFiles, setReceivedFiles] = useState<string[]>([]);
  const [selectedDevice, setSelectedDevice] = useState<Device | null>(null);
  const [filePathInput, setFilePathInput] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [connecting, setConnecting] = useState(false);
  const [daemonAvailable, setDaemonAvailable] = useState<boolean | null>(null);

  // 1. 探测本机守护进程是否在线
  useEffect(() => {
    fetchWhoAmI()
      .then((info) => {
        setMe(info);
        setDaemonAvailable(true);
      })
      .catch(() => {
        setDaemonAvailable(false);
      });
  }, []);

  // 2. 拉取设备列表（每 3 秒）
  useEffect(() => {
    if (!daemonAvailable) return;
    const refresh = () =>
      fetchDevices()
        .then(setDevices)
        .catch((e) => setError(String(e)));
    refresh();
    const id = setInterval(refresh, 3000);
    return () => clearInterval(id);
  }, [daemonAvailable]);

  // 3. 订阅 WS 事件：按 event_type 分发（进度 / 传入请求 / 决议完成）
  useEffect(() => {
    if (!daemonAvailable) return;
    const unsubscribe = subscribeEvents((ev) => {
      if (ev.event_type === 'progress') {
        setProgress((prev) => ({ ...prev, [ev.file_id]: ev }));
      } else if (ev.event_type === 'incoming') {
        setIncoming((prev) => ({ ...prev, [ev.incoming_id]: ev }));
      } else if (ev.event_type === 'incoming_resolved') {
        setIncoming((prev) => {
          const { [ev.incoming_id]: _removed, ...rest } = prev;
          return rest;
        });
      }
    });
    return unsubscribe;
  }, [daemonAvailable]);

  // 3.5 启动时拉取一次待决请求（错过 WS 推送的兜底）
  useEffect(() => {
    if (!daemonAvailable) return;
    listIncoming()
      .then((entries) => {
        const map: Record<string, IncomingEntry> = {};
        for (const e of entries) map[e.incoming_id] = e;
        setIncoming(map);
      })
      .catch(() => {});
  }, [daemonAvailable]);

  // 4. 接收文件列表（每 5 秒刷新）
  useEffect(() => {
    if (!daemonAvailable) return;
    const refresh = () =>
      listReceivedFiles()
        .then(setReceivedFiles)
        .catch(() => {});
    refresh();
    const id = setInterval(refresh, 5000);
    return () => clearInterval(id);
  }, [daemonAvailable]);

  const handleSend = useCallback(async () => {
    if (!selectedDevice || !filePathInput) return;
    setConnecting(true);
    setError(null);
    try {
      await sendFile(
        selectedDevice.ip,
        selectedDevice.transfer_port,
        filePathInput,
        selectedDevice.gateway_port
      );
      setFilePathInput('');
    } catch (e) {
      setError(String(e));
    } finally {
      setConnecting(false);
    }
  }, [selectedDevice, filePathInput]);

  const handleCancel = useCallback((fileId: string) => {
    cancelTransfer(fileId).catch((e) => setError(String(e)));
  }, []);

  const handleAccept = useCallback((incomingId: string) => {
    acceptIncoming(incomingId).catch((e) => setError(String(e)));
  }, []);

  const handleReject = useCallback((incomingId: string) => {
    rejectIncoming(incomingId).catch((e) => setError(String(e)));
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    if (daemonAvailable) {
      // 本机模式：使用路径
      const files = Array.from(e.dataTransfer.files);
      if (files.length > 0) {
        // Note: 出于安全考虑，浏览器无法获取本地文件完整路径，
        // 这里只能走本机 daemon 直接传 / 已接收文件夹中的文件。
        const f = files[0];
        // 在本机 daemon 模式下，提示用户输入绝对路径
        setError(`浏览器安全限制无法读取文件路径，请在下方输入框填入绝对路径。文件名: ${f.name} 大小: ${formatBytes(f.size)}`);
      }
      return;
    }
    // 纯浏览器模式：使用 fetch 上传（TODO）
    setError('纯浏览器模式尚未实现，请启动本机守护进程');
  }, [daemonAvailable]);

  const transfers = Object.values(progress);

  return (
    <div className="app">
      <header className="header">
        <h1>FTCore</h1>
        <span className="subtitle">局域网文件传输</span>
        <div className="badge-row">
          <span className={`badge ${daemonAvailable ? 'ok' : 'warn'}`}>
            {daemonAvailable === null
              ? '检测守护进程…'
              : daemonAvailable
              ? '守护进程已连接'
              : '守护进程未运行 - 纯浏览器模式'}
          </span>
        </div>
      </header>

      <section className="panel">
        <h2>本机</h2>
        {me ? (
          <div className="me">
            <div>名称: <strong>{me.name}</strong></div>
            <div>平台: <strong>{me.platform}</strong></div>
            <div>HTTP 网关: <strong>:{me.gateway_port}</strong></div>
            <div>传输端口: <strong>:{me.transfer_port}</strong></div>
          </div>
        ) : (
          <div className="me">
            {daemonAvailable === false
              ? '未连接到本机守护进程。运行 `ftcore-cli daemon` 后重启本页。'
              : '加载中…'}
          </div>
        )}
      </section>

      <section className="panel">
        <h2>设备列表 ({devices.length})</h2>
        {devices.length === 0 ? (
          <p className="muted">未发现设备，请确认对端已启动并处于同一局域网。</p>
        ) : (
          <ul className="device-list">
            {devices.map((d) => (
              <li
                key={d.id}
                className={selectedDevice?.id === d.id ? 'selected' : ''}
                onClick={() => setSelectedDevice(d)}
              >
                <div className="device-row">
                  <span className="device-name">{d.name}</span>
                  <span className="device-meta">
                    {d.platform} · {d.ip}:{d.transfer_port}
                  </span>
                </div>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section
        className="panel dropzone"
        onDragOver={(e) => e.preventDefault()}
        onDrop={handleDrop}
      >
        <h2>发送文件</h2>
        {selectedDevice ? (
          <div>
            <div className="muted">
              目标: <strong>{selectedDevice.name}</strong> ({selectedDevice.ip})
            </div>
            <div className="send-row">
              <input
                type="text"
                placeholder="文件绝对路径，如 C:\Users\me\video.mp4"
                value={filePathInput}
                onChange={(e) => setFilePathInput(e.target.value)}
                className="text-input"
                disabled={!daemonAvailable}
              />
              <button
                onClick={handleSend}
                disabled={!filePathInput || connecting || !daemonAvailable}
              >
                发送
              </button>
            </div>
            <div className="hint">（也可将文件拖到此处获取文件名提示）</div>
          </div>
        ) : (
          <p className="muted">请先选择目标设备</p>
        )}
      </section>

      {Object.keys(incoming).length > 0 && (
        <section className="panel">
          <h2>传入请求 ({Object.keys(incoming).length})</h2>
          <ul className="incoming-list">
            {Object.values(incoming).map((e) => (
              <li key={e.incoming_id} className="incoming">
                <div className="t-row">
                  <span className="t-name">{e.file_name}</span>
                </div>
                <div className="t-meta">
                  来自 {e.from_name} ({e.from_ip}) · {formatBytes(e.file_size)}
                  {e.sha256 && (
                    <span className="muted"> · SHA256 {e.sha256.slice(0, 12)}…</span>
                  )}
                </div>
                <div className="incoming-actions">
                  <button className="danger" onClick={() => handleReject(e.incoming_id)}>
                    拒绝
                  </button>
                  <button className="primary" onClick={() => handleAccept(e.incoming_id)}>
                    接受
                  </button>
                </div>
              </li>
            ))}
          </ul>
        </section>
      )}

      <section className="panel">
        <h2>传输进度 ({transfers.length})</h2>
        {transfers.length === 0 ? (
          <p className="muted">暂无传输任务</p>
        ) : (
          <ul className="transfer-list">
            {transfers
              .slice()
              .sort((a) => (a.status === 'InProgress' ? -1 : 1))
              .map((p) => (
                <li key={p.file_id} className={`transfer ${p.status}`}>
                  <div className="t-row">
                    <span className="t-name">
                      {p.incoming ? '[收] ' : ''}
                      {p.file_name}
                    </span>
                    <span className="t-status">{p.status}</span>
                    {p.status === 'InProgress' && (
                      <button onClick={() => handleCancel(p.file_id)}>
                        取消
                      </button>
                    )}
                  </div>
                  <div className="t-progress">
                    <div
                      className="t-bar"
                      style={{
                        width: `${
                          p.file_size > 0
                            ? (p.bytes_transferred / p.file_size) * 100
                            : 0
                        }%`,
                      }}
                    />
                  </div>
                  <div className="t-meta">
                    {formatBytes(p.bytes_transferred)} / {formatBytes(p.file_size)}
                    {p.status === 'InProgress' && (
                      <> · {formatSpeed(p.speed_bps)}</>
                    )}
                    {p.error && <span className="t-error"> · {p.error}</span>}
                  </div>
                </li>
              ))}
          </ul>
        )}
      </section>

      <section className="panel">
        <h2>已接收文件 ({receivedFiles.length})</h2>
        {receivedFiles.length === 0 ? (
          <p className="muted">无</p>
        ) : (
          <ul className="file-list">
            {receivedFiles.map((name) => (
              <li key={name}>
                <a href={`/api/files/${encodeURIComponent(name)}`} download>
                  {name}
                </a>
              </li>
            ))}
          </ul>
        )}
      </section>

      {error && (
        <div className="error-banner">
          {error}
          <button onClick={() => setError(null)}>×</button>
        </div>
      )}
    </div>
  );
}
