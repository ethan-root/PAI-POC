# 使用官方 Python 基础镜像
FROM python:3.9-slim

# 【关键修改 1】设置环境变量
# 让 Python 的 print 输出不经过缓存直接打印到控制台
# 这样你在 PAI 的日志里能立刻看到报错，而不是等程序挂了都看不到日志
ENV PYTHONUNBUFFERED=1

# 设置工作目录
WORKDIR /app

# 【建议修改】如果有依赖包文件，取消下面两行的注释
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# 【关键修改 2】复制当前目录所有文件
# 使用 . . 比 main.py 更通用，防止你以后加了配置文件忘了 copy
COPY . .

# 【关键修改 3】声明暴露端口
# 明确告诉 PAI：“我这个容器会监听 8000 端口”
EXPOSE 8000

# 运行命令
# 加上 -u 参数 (unbuffered) 是双重保险，确保日志实时输出
# ❌ 暂时注释掉原来的启动命令
# CMD ["python", "-u", "main.py"]

# ✅ 换成这个“休眠命令”
# 让容器先睡 1 个小时，这样它绝对不会 crash，PAI 状态会变成 Running
CMD ["sleep", "3600"]