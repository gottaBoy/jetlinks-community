#!/bin/bash
# ziot deploy script
#
# 使用方式：
#   1. IDEA 中先 mvn clean package -DskipTests  产出 JAR
#   2. ./scripts/deploy.sh 0.0.1       # 构建 + 推送 + 更新 k8s tag
#   3. ./scripts/deploy.sh 0.0.1 --sync    # 同上 + 触发 ArgoCD 同步
#
# 前置条件：
#   - Maven 已编译: jetlinks-standalone/target/application.jar
#   - docker login harbor.intra.zeron.ai
#   - kubectl 可用（--sync 时需要）
set -euo pipefail

TAG="${1:-latest}"
SYNC="${2:-}"
REPO="harbor.intra.zeron.ai/smartdrive/ziot"
KUBECONFIG="${KUBECONFIG:-/Users/minyi/kube.conf}"
JAR_PATH="jetlinks-standalone/target/application.jar"

cd "$(dirname "$0")/.."

# ── 1. 检查 JAR 是否已编译 ──
if [ ! -f "$JAR_PATH" ]; then
  echo "=== JAR 不存在，开始 Maven 编译 ==="
  ./mvnw clean package -DskipTests -pl jetlinks-standalone -am
fi

echo "=== Build ${REPO}:${TAG} ==="
docker build --platform linux/amd64 -t "${REPO}:${TAG}" .

echo "=== Push ==="
docker push "${REPO}:${TAG}"

echo "=== Update k8s image tag ==="
cd k8s/overlays/production
if [[ "$OSTYPE" == "darwin"* ]]; then
  sed -i '' "s/newTag:.*/newTag: ${TAG}/" kustomization.yaml
else
  sed -i "s/newTag:.*/newTag: ${TAG}/" kustomization.yaml
fi
cd ../../..

if [ "$SYNC" = "--sync" ]; then
  echo "=== Trigger ArgoCD sync ==="
  export KUBECONFIG="${KUBECONFIG}"
  kubectl --kubeconfig=/Users/minyi/kube.conf apply -k k8s/overlays/production
  # kubectl -n argocd patch application ziot \
  #   --type=merge -p '{"operation":{"sync":{"revision":"master"}}}'
fi

echo "=== Done ==="
echo "Check: kubectl --kubeconfig=${KUBECONFIG} -n zota get pods -l app=ziot"
