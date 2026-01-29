# 使用官方 Python 基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 复制当前目录代码到容器
COPY main.py .

# 运行命令
CMD ["python", "main.py"]