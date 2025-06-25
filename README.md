# Nexus-cli Linux内存优化版

有问题联系推特：https://x.com/hua_web3

Nexus Network CLI 证明器的Linux内存优化版本。

## 安装

### 1. 安装Rust环境（有的话直接第2步）
```bash
# 安装Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.bashrc
```

### 2. 克隆并编译
```bash
# 克隆并编译
git clone https://github.com/huahua1220/nexus-cli-linux.git
cd nexus-cli-linux
cargo build --release
```

## 使用方法

### 准备节点文件
运行前先打开 `target/release/nodes.txt` 文件，一行一个节点ID

### 批量模式
```bash
# 运行批量挖矿
./target/release/nexus batch-file --file nodes.txt --max-concurrent 节点数
```

## 主要改进

- 内存优化，单个线程比官方版节省30%-50%内存占用
- 批量节点id启动，防止单个节点id出现429等错误影响效率
