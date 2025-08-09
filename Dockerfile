FROM registry-1.docker.io/library/python:3.11-slim

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*
COPY . .

# 先复制依赖文件
COPY pyproject.toml uv.lock ./

# 安装uv包管理器
RUN pip install uv

# 使用uv安装Python依赖
RUN uv pip install --system --no-cache -r pyproject.toml

# 启动命令
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
