# ziot Docker image
# Build: ./mvnw clean package -DskipTests -pl jetlinks-standalone
# Then:  docker build -t harbor.intra.zeron.ai/smartdrive/ziot:0.0.1 .
FROM harbor.intra.zeron.ai/library/eclipse-temurin:17-jre

WORKDIR /home/ziot
COPY jetlinks-standalone/target/application.jar /home/ziot/app.jar
EXPOSE 8848

ENV JAVA_OPTS="-Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 \
  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/ \
  -Dreactor.schedulers.defaultBoundedElasticOnVirtualThreads=true \
  -Djava.security.egd=file:/dev/./urandom"
ENV TZ=Asia/Shanghai

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens java.base/java.io=ALL-UNNAMED \
  --add-opens java.base/java.net=ALL-UNNAMED \
  --add-opens java.base/java.text=ALL-UNNAMED \
  --add-opens java.base/java.math=ALL-UNNAMED \
  --add-opens java.scripting/javax.script=ALL-UNNAMED \
  --add-opens java.base/java.time=ALL-UNNAMED \
  -jar /home/ziot/app.jar"]