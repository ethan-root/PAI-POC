import http.server
import socketserver
import os

# PAI 默认检查的端口通常是 8000
PORT = 8000

class MyHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # 当收到 GET 请求时，返回 200 OK 和一段文字
        self.send_response(200)
        self.send_header('Content-type', 'text/plain; charset=utf-8')
        self.end_headers()
        # 这里打印当前的版本号（如果有环境变量）或者简单的 Hello
        self.wfile.write(b"Hello! PAI Deployment Success! Version: V2.0\n")

# 监听 0.0.0.0 (必须是这个，不能是 localhost)
# 端口 8000
with socketserver.TCPServer(("0.0.0.0", PORT), MyHandler) as httpd:
    print(f"Serving at port {PORT}")
    # 这一句最重要：让程序一直运行，不要退出
    httpd.serve_forever()