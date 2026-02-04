import http.server
import socketserver
import sys

# 1. 强制监听 0.0.0.0 (不要用 localhost)
HOST = "0.0.0.0"
# 2. 端口设置为 8000 (PAI 默认常用端口)
PORT = 8000

class HealthCheckHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # 只要收到请求，就返回 200 OK，告诉 PAI "我活着"
        self.send_response(200)
        self.send_header('Content-type', 'text/plain; charset=utf-8')
        self.end_headers()
        self.wfile.write(b"PAI Service is Running! Status: OK")

    # 避免日志太吵，可以屏蔽 log_message (可选)
    # def log_message(self, format, *args):
    #     pass

if __name__ == "__main__":
    try:
        # 3. 打印启动日志 (加 flush=True 确保能立刻在控制台看到)
        print(f"🚀 Starting server on {HOST}:{PORT}...", flush=True)
        
        # 4. 启动服务
        server = socketserver.TCPServer((HOST, PORT), HealthCheckHandler)
        print("✅ Server started successfully! Waiting for PAI health check...", flush=True)
        
        # 5. 永久运行 (死循环，除非报错否则不退出)
        server.serve_forever()
        
    except Exception as e:
        print(f"❌ Server crashed: {e}", flush=True)
        sys.exit(1)