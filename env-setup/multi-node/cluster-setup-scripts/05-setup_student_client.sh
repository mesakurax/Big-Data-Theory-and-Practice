#!/bin/bash
# 配置学生 Hadoop 远程客户端环境脚本
# 用法: ./05-setup_student_client.sh [学号] [集群主节点IP/主机名]
# 此脚本用于在学生本地机器上配置 Hadoop 客户端以远程访问集群

STUDENT_ID="$1"
CLUSTER_MASTER="$2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATES_DIR="$SCRIPT_DIR/templates"

# 默认集群主节点地址
DEFAULT_CLUSTER_MASTER="hadoop-master"

# 如果没有提供学号，提示用户输入
if [ -z "$STUDENT_ID" ]; then
    echo "请输入您的学号:"
    read -r STUDENT_ID
fi

# 如果没有提供集群地址，使用默认值或提示输入
if [ -z "$CLUSTER_MASTER" ]; then
    echo "请输入集群主节点地址 (默认: $DEFAULT_CLUSTER_MASTER):"
    read -r input_master
    CLUSTER_MASTER="${input_master:-$DEFAULT_CLUSTER_MASTER}"
fi

# 验证学号格式（假设学号为数字）
if [[ ! "$STUDENT_ID" =~ ^[0-9]+$ ]]; then
    echo "错误: 学号格式不正确，请输入数字学号"
    exit 1
fi

# 创建模板目录
mkdir -p "$TEMPLATES_DIR"

echo "=== 开始配置学生 Hadoop 远程客户端环境 ==="
echo "学号: $STUDENT_ID"
echo "集群主节点: $CLUSTER_MASTER"
echo "模板目录: $TEMPLATES_DIR"
echo

# 网络连接测试函数
test_network_connectivity() {
    echo "测试网络连接..."
    
    # 测试主机名解析
    if ! nslookup "$CLUSTER_MASTER" >/dev/null 2>&1 && ! ping -c 1 "$CLUSTER_MASTER" >/dev/null 2>&1; then
        echo "警告: 无法解析主机名 $CLUSTER_MASTER"
        echo "请确保:"
        echo "1. 网络连接正常"
        echo "2. 主机名/IP 地址正确"
        echo "3. 防火墙允许相关端口访问"
        return 1
    fi
    
    # 测试关键端口连通性
    local ports=("9000" "8088" "9870")
    local failed_ports=()
    
    for port in "${ports[@]}"; do
        if ! timeout 5 bash -c "</dev/tcp/$CLUSTER_MASTER/$port" 2>/dev/null; then
            failed_ports+=("$port")
        fi
    done
    
    if [ ${#failed_ports[@]} -gt 0 ]; then
        echo "警告: 以下端口无法连接: ${failed_ports[*]}"
        echo "这可能影响 Hadoop 客户端的正常使用"
        return 1
    fi
    
    echo "网络连接测试通过"
    return 0
}

# 执行网络连接测试
test_network_connectivity

# 创建配置文件模板
echo "创建配置文件模板..."

# core-site.xml 模板
cat > "$TEMPLATES_DIR/core-site.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://$CLUSTER_MASTER:9000</value>
        <description>HDFS 默认文件系统</description>
    </property>
    
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/tmp/hadoop-\${user.name}</value>
        <description>Hadoop 临时目录</description>
    </property>
    
    <property>
        <name>fs.trash.interval</name>
        <value>1440</value>
        <description>回收站保留时间（分钟）</description>
    </property>
    
    <property>
        <name>hadoop.security.authentication</name>
        <value>simple</value>
        <description>认证方式</description>
    </property>
    
    <property>
        <name>hadoop.proxyuser.hadoop.hosts</name>
        <value>*</value>
        <description>代理用户主机</description>
    </property>
    
    <property>
        <name>hadoop.proxyuser.hadoop.groups</name>
        <value>*</value>
        <description>代理用户组</description>
    </property>
</configuration>
EOF

# yarn-site.xml 模板
cat > "$TEMPLATES_DIR/yarn-site.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>$CLUSTER_MASTER</value>
        <description>ResourceManager 主机名</description>
    </property>
    
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>$CLUSTER_MASTER:8032</value>
        <description>ResourceManager 地址</description>
    </property>
    
    <property>
        <name>yarn.resourcemanager.scheduler.address</name>
        <value>$CLUSTER_MASTER:8030</value>
        <description>ResourceManager 调度器地址</description>
    </property>
    
    <property>
        <name>yarn.resourcemanager.resource-tracker.address</name>
        <value>$CLUSTER_MASTER:8031</value>
        <description>ResourceManager 资源跟踪器地址</description>
    </property>
    
    <property>
        <name>yarn.resourcemanager.admin.address</name>
        <value>$CLUSTER_MASTER:8033</value>
        <description>ResourceManager 管理地址</description>
    </property>
    
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <value>$CLUSTER_MASTER:8088</value>
        <description>ResourceManager Web UI 地址</description>
    </property>
    
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
        <description>NodeManager 辅助服务</description>
    </property>
    
    <property>
        <name>yarn.application.classpath</name>
        <value>\$HADOOP_CONF_DIR,\$HADOOP_COMMON_HOME/share/hadoop/common/*,\$HADOOP_COMMON_HOME/share/hadoop/common/lib/*,\$HADOOP_HDFS_HOME/share/hadoop/hdfs/*,\$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*,\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*,\$HADOOP_YARN_HOME/share/hadoop/yarn/*,\$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*</value>
        <description>应用程序类路径</description>
    </property>
</configuration>
EOF

# mapred-site.xml 模板
cat > "$TEMPLATES_DIR/mapred-site.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
        <description>MapReduce 框架</description>
    </property>
    
    <property>
        <name>mapreduce.jobhistory.address</name>
        <value>$CLUSTER_MASTER:10020</value>
        <description>JobHistory 服务地址</description>
    </property>
    
    <property>
        <name>mapreduce.jobhistory.webapp.address</name>
        <value>$CLUSTER_MASTER:19888</value>
        <description>JobHistory Web UI 地址</description>
    </property>
    
    <property>
        <name>mapreduce.application.classpath</name>
        <value>\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
        <description>MapReduce 应用程序类路径</description>
    </property>
    
    <property>
        <name>mapreduce.job.queuename</name>
        <value>students</value>
        <description>默认提交到 students 队列</description>
    </property>
</configuration>
EOF

echo "配置文件模板创建完成"
echo

# 配置本地客户端环境
echo "为学生 $STUDENT_ID 配置本地客户端环境..."

# 创建本地 Hadoop 配置目录
hadoop_conf_dir="$HOME/.hadoop/conf"
mkdir -p "$hadoop_conf_dir"

# 复制配置文件
cp "$TEMPLATES_DIR/core-site.xml" "$hadoop_conf_dir/"
cp "$TEMPLATES_DIR/yarn-site.xml" "$hadoop_conf_dir/"
cp "$TEMPLATES_DIR/mapred-site.xml" "$hadoop_conf_dir/"

echo "配置文件已复制到: $hadoop_conf_dir"

# 检查是否已经配置过环境变量
if ! grep -q "# Hadoop 远程客户端环境配置" "$HOME/.bashrc" 2>/dev/null; then
    echo "配置环境变量..."
    
    # 配置环境变量
    cat >> "$HOME/.bashrc" << EOF

# Hadoop 远程客户端环境配置 (自动添加 - $STUDENT_ID)
export HADOOP_HOME=\${HADOOP_HOME:-/opt/hadoop}
export HADOOP_CONF_DIR=\$HOME/.hadoop/conf
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin
export JAVA_HOME=\${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}
export HADOOP_USER_NAME=$STUDENT_ID
export HADOOP_MAPRED_QUEUE_NAME=students

# Hadoop 便捷别名
alias hls='hdfs dfs -ls'
alias hput='hdfs dfs -put'
alias hget='hdfs dfs -get'
alias hmkdir='hdfs dfs -mkdir'
alias hrm='hdfs dfs -rm'
alias hcat='hdfs dfs -cat'
alias hdu='hdfs dfs -du -h'
alias hquota='hdfs dfs -count -q'
alias hmy='hdfs dfs -ls /users/$STUDENT_ID'
alias hcd='hdfs dfs -ls'

# 快速进入个人目录
alias hmydir='hdfs dfs -ls /users/$STUDENT_ID'
alias hmyput='hdfs dfs -put \$1 /users/$STUDENT_ID/'
alias hmyget='hdfs dfs -get /users/$STUDENT_ID/\$1 .'
EOF

    echo "环境变量配置完成"
else
    echo "环境变量已存在，跳过配置"
fi

# 创建客户端测试脚本
test_script="$HOME/.hadoop/test_connection.sh"
cat > "$test_script" << EOF
#!/bin/bash
# Hadoop 客户端连接测试脚本

echo "=== Hadoop 客户端连接测试 ==="
echo "学号: $STUDENT_ID"
echo "集群地址: $CLUSTER_MASTER"
echo

# 设置环境变量
export HADOOP_USER_NAME=$STUDENT_ID

echo "1. 测试 HDFS 连接..."
if hdfs dfs -ls / >/dev/null 2>&1; then
    echo "   ✓ HDFS 连接成功"
else
    echo "   ✗ HDFS 连接失败"
    echo "   请检查网络连接和配置文件"
fi

echo "2. 测试个人目录访问..."
if hdfs dfs -ls /users/$STUDENT_ID >/dev/null 2>&1; then
    echo "   ✓ 个人目录访问成功"
else
    echo "   ✗ 个人目录访问失败"
    echo "   请联系管理员确认账户已创建"
fi

echo "3. 测试 YARN 连接..."
if yarn application -list >/dev/null 2>&1; then
    echo "   ✓ YARN 连接成功"
else
    echo "   ✗ YARN 连接失败"
    echo "   请检查 ResourceManager 配置"
fi

echo "4. 查看存储配额..."
hdfs dfs -count -q /users/$STUDENT_ID 2>/dev/null || echo "   无法获取配额信息"

echo
echo "测试完成。如有问题，请联系管理员。"
EOF

chmod +x "$test_script"

echo "学生 $STUDENT_ID 的远程客户端环境配置完成"

echo
echo "=== 远程客户端环境配置完成 ==="
echo "学号: $STUDENT_ID"
echo "集群地址: $CLUSTER_MASTER"
echo
echo "配置内容:"
echo "- Hadoop 配置文件: ~/.hadoop/conf/"
echo "- 环境变量: ~/.bashrc"
echo "- 用户身份: $STUDENT_ID"
echo "- 默认队列: students"
echo "- 测试脚本: ~/.hadoop/test_connection.sh"
echo
echo "使用说明:"
echo "1. 重新加载环境变量: source ~/.bashrc"
echo "2. 运行连接测试: ~/.hadoop/test_connection.sh"
echo "3. 测试 HDFS 访问: hdfs dfs -ls /"
echo "4. 查看个人目录: hdfs dfs -ls /users/$STUDENT_ID"
echo "5. 提交测试作业: hadoop jar \$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /public/data/wordcount/simple-test.txt /users/$STUDENT_ID/output/test-\$(date +%Y%m%d-%H%M%S)"
echo
echo "常用别名:"
echo "- hls: 列出 HDFS 目录"
echo "- hput: 上传文件到 HDFS"
echo "- hget: 从 HDFS 下载文件"
echo "- hmydir: 查看个人目录"
echo "- hmyput: 上传文件到个人目录"
echo "- hmyget: 从个人目录下载文件"
echo
echo "如有问题，请联系管理员或查看连接测试结果。"