#!/usr/bin/env bash

# 检查 Java 版本
java_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2)
echo "Java 版本: $java_version"

# 设置堆内存（根据实际情况调整）
export JAVA_OPTS="-Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
# OOM 时 dump 堆快照，方便排查
export JAVA_OPTS="$JAVA_OPTS -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=data/logs/"
# 日志路径由 application.yml 的 logging.file.name/path 控制，无需 env 指定

# 启动应用（业务日志由 logback 接管，启动错误保留到 startup.log 方便排查）
nohup java $JAVA_OPTS -server \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens java.base/java.io=ALL-UNNAMED \
  --add-opens java.base/java.net=ALL-UNNAMED \
  --add-opens java.base/java.text=ALL-UNNAMED \
  --add-opens java.base/java.math=ALL-UNNAMED \
  --add-opens java.scripting/javax.script=ALL-UNNAMED \
  --add-opens java.base/java.time=ALL-UNNAMED \
  -Dreactor.schedulers.defaultBoundedElasticOnVirtualThreads=true \
  -Djava.security.egd=file:/dev/./urandom \
  -jar application.jar \
  > /dev/null \
  2> data/logs/startup.log &

echo "应用已启动，进程ID: $!"
echo "查看日志: tail -f data/logs/ziot_8848.log"
#  --spring.profiles.active=prod \
#  --server.port=8080