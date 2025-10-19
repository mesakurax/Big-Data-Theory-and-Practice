# HDFS 文件管理器

## 项目简介

这是一个基于 Hadoop Java SDK 开发的 HDFS 文件管理器，提供了完整的 HDFS 文件操作功能，包括文件上传、下载、删除、目录遍历和统计等功能。程序采用命令行交互式界面，方便用户进行各种 HDFS 操作。

## 功能特性

1. **文件上传**：将本地文件上传到 HDFS，支持覆盖已存在的文件，自动创建目标目录
2. **文件下载**：从 HDFS 下载文件到本地，支持覆盖已存在的本地文件
3. **文件删除**：删除 HDFS 中的文件或目录，支持递归删除目录
4. **目录遍历**：递归列出目录中的所有文件和子目录，显示文件大小和类型
5. **目录统计**：统计目录中的文件数量、子目录数量和总存储空间
6. **自动测试**：一键运行完整的功能测试，验证所有核心功能

## 环境要求

- **Hadoop**: 3.3.x 单节点伪分布式集群
- **Java**: JDK 8 或更高版本
- **Maven**: 3.6 或更高版本
- **网络**: NameNode Web UI 可访问 (http://localhost:9870/)

## 安装与编译

### 1. 进入项目目录

```bash
cd /home/hadoop/hdfs-file-manager
```

### 2. 编译项目

```bash
mvn clean compile
```

### 3. 打包项目

```bash
mvn clean package
```

编译成功后，会在 `target` 目录下生成 `hdfs-file-manager-1.0.0.jar` 文件。

## 使用方法

### 方式一：交互式运行

```bash
# 在项目根目录下运行
mvn exec:java -Dexec.mainClass="com.bigdata.hdfs.HDFSFileManager"
```

启动后会显示菜单，输入数字选择对应功能：
- 输入 `1` - 上传文件到 HDFS（自动使用默认配置）
- 输入 `2` - 从 HDFS 下载文件（自动使用默认配置）
- 输入 `3` - 删除 HDFS 文件/目录（自动使用默认配置）
- 输入 `4` - 列出目录内容（自动使用默认配置）
- 输入 `5` - 统计目录信息（自动使用默认配置）
- 输入 `6` - 运行自动测试（完整测试所有功能）
- 输入 `0` - 退出程序

**注意**：所有功能均使用预设的默认值，无需手动输入路径和参数。

### 方式二：非交互式运行（自动测试）

```bash
# 自动执行完整测试，无需任何输入
echo "" | mvn exec:java -Dexec.mainClass="com.bigdata.hdfs.HDFSFileManager"
```

### 方式三：使用 Java 命令运行

```bash
# 确保 HADOOP_HOME 环境变量已设置
java -cp "target/hdfs-file-manager-1.0.0.jar:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*" com.bigdata.hdfs.HDFSFileManager
```

## 默认配置

程序提供了以下默认值，方便快速测试：

### 本地文件
- **默认测试文件**: `default_test_file.txt` (位于项目根目录 `/home/hadoop/hdfs-file-manager/`)
- **默认下载路径**: `downloaded_file.txt` (保存到项目根目录)

### HDFS 路径
- **默认上传目录**: `/user/student/project/input`
- **默认文件路径**: `/user/student/project/input/default_test_file.txt`
- **自动测试目录**: `/user/student/project/test_<timestamp>` (包含时间戳，确保唯一性)

### 操作模式
- **覆盖模式**: 默认为 `是`（文件存在时自动覆盖）
- **递归删除**: 删除文件时默认为 `否`，删除目录时为 `是`

## 主菜单

```
==================================================
           HDFS 文件管理器菜单
==================================================
1. 上传文件到 HDFS
2. 从 HDFS 下载文件
3. 删除 HDFS 文件/目录
4. 列出目录内容
5. 统计目录信息
6. 运行自动测试
0. 退出程序
==================================================
```

## 功能说明

### 功能 1: 上传文件到 HDFS

**默认配置**:
- 本地文件: `default_test_file.txt`
- HDFS 路径: `/user/student/project/input/default_test_file.txt`
- 覆盖模式: 是

**执行示例**:
```
========== 文件上传 ==========
📁 本地文件路径: /home/hadoop/hdfs-file-manager/default_test_file.txt
📦 文件名称: default_test_file.txt
📊 文件大小: 5.12 KB
🎯 HDFS 目标路径: /user/student/project/input/default_test_file.txt
🔄 覆盖模式: 是（如果文件存在将被覆盖）

⏳ 开始上传...
✅ 上传成功！
```

### 功能 2: 从 HDFS 下载文件

**默认配置**:
- HDFS 路径: `/user/student/project/input/default_test_file.txt`
- 本地路径: `downloaded_file.txt`
- 覆盖模式: 是

### 功能 3: 删除 HDFS 文件/目录

**默认配置**:
- HDFS 路径: `/user/student/project/input/default_test_file.txt`
- 递归删除: 否（仅删除文件）

### 功能 4: 列出目录内容

**默认配置**:
- HDFS 目录: `/user/student/project/input`

**输出示例**:
```
========== 列出目录内容 ==========
🎯 HDFS 目录路径: /user/student/project/input

📂 目录结构:
----------------------------
[FILE] default_test_file.txt (5.12 KB)
[FILE] test_data.csv (2.35 MB)
----------------------------
```

### 功能 5: 统计目录信息

**默认配置**:
- HDFS 目录: `/user/student/project/input`

**输出示例**:
```
========== 统计目录信息 ==========
🎯 HDFS 目录路径: /user/student/project/input

⏳ 正在统计...

📊 目录统计信息:
📊   文件数量: 15
📊   目录数量: 3
📊   总大小: 5.67 MB
```

### 功能 6: 运行自动测试

自动测试会执行以下步骤：
1. 创建本地测试文件（100行测试数据）
2. 上传文件到 HDFS
3. 列出目录内容
4. 统计目录信息
5. 下载文件到本地
6. 删除测试文件和目录
7. 清理本地临时文件

每个步骤都会显示详细的执行信息，包括文件路径、大小、操作模式等。

## Web UI 验证

所有操作完成后，可以通过 NameNode Web UI 进行验证：

1. 打开浏览器访问: http://localhost:9870/
2. 点击 "Utilities" -> "Browse the file system"
3. 导航到 `/user/student/project/input` 查看文件
4. 验证操作结果

## 常见问题

### 1. 连接 HDFS 失败

**错误**: `连接 HDFS 失败: Connection refused`

**解决方案**:
- 确认 Hadoop 集群已启动: `jps` 查看 NameNode 和 DataNode 是否运行
- 检查 HDFS URI 是否正确（默认为 `hdfs://localhost:9000`）
- 验证防火墙设置和端口是否开放

### 2. 权限拒绝

**错误**: `Permission denied`

**解决方案**:
```bash
# 创建用户目录并设置权限
hdfs dfs -mkdir -p /user/student/project/input
hdfs dfs -chmod -R 755 /user/student/project

# 或者以超级用户身份修改所有者
sudo -u hdfs hdfs dfs -chown -R $USER:$USER /user/student/project
```

### 3. 找不到默认测试文件

**错误**: `File not found: default_test_file.txt`

**解决方案**:
- 确保在项目根目录 `/home/hadoop/hdfs-file-manager` 下运行程序
- 检查 `default_test_file.txt` 文件是否存在
- 如不存在，程序会自动报错，可以手动创建该文件或使用功能6自动测试

## 项目结构

```
hdfs-file-manager/
├── pom.xml                          # Maven 配置文件
├── README.md                        # 项目说明文档
├── default_test_file.txt           # 默认测试文件
├── src/
│   └── main/
│       ├── java/
│       │   └── com/bigdata/hdfs/
│       │       └── HDFSFileManager.java  # 主程序类
│       └── resources/
│           └── logback.xml          # 日志配置文件
├── target/                          # 编译输出目录
└── logs/                            # 日志文件目录
```

## 日志说明

程序运行时会生成日志文件：`logs/hdfs-file-manager.log`

日志包含：
- 所有操作的详细记录
- 错误信息和堆栈跟踪
- HDFS 连接信息

## 技术实现

### 核心类和方法

- **HDFSFileManager**: 主类，包含所有 HDFS 操作方法
- **DirectoryStats**: 目录统计信息类
- **FileSystem**: Hadoop 文件系统接口
- **Configuration**: Hadoop 配置对象

### 依赖项

- `hadoop-client`: 3.3.4
- `slf4j-api`: 1.7.36
- `logback-classic`: 1.2.12

## 作者信息

- **课程**: 大数据理论与实践
- **用户**: 522025320186 徐国伟
- **练习**: 练习一 - HDFS 常见操作
- **日期**: 2025年

## 许可证

本项目仅用于教学目的，遵循课程要求。
