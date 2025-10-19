package com.bigdata.hdfs;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS 文件管理器
 * 提供基本的 HDFS 文件操作功能
 */
public class HDFSFileManager {
    private static final Logger logger = LoggerFactory.getLogger(HDFSFileManager.class);

    private FileSystem fileSystem;
    private Configuration configuration;

    /**
     * 构造函数，初始化 HDFS 连接
     * @param hdfsUri HDFS 的 URI，例如 "hdfs://localhost:9000"
     */
    public HDFSFileManager(String hdfsUri) throws IOException {
        // 1. 创建 Configuration 对象
        configuration = new Configuration();
        
        // 2. 设置 HDFS URI
        configuration.set("fs.defaultFS", hdfsUri);
        
        // 3. 获取 FileSystem 实例
        try {
            fileSystem = FileSystem.get(URI.create(hdfsUri), configuration);
            logger.info("成功连接到 HDFS: {}", hdfsUri);
        } catch (IOException e) {
            logger.error("连接 HDFS 失败: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * 上传本地文件到 HDFS
     * @param localPath 本地文件路径
     * @param hdfsPath HDFS 目标路径
     * @param overwrite 是否覆盖已存在的文件
     * @return 上传是否成功
     */
    public boolean uploadFile(String localPath, String hdfsPath, boolean overwrite) {
        try {
            // 1. 检查本地文件是否存在
            File localFile = new File(localPath);
            if (!localFile.exists()) {
                logger.error("本地文件不存在: {}", localPath);
                return false;
            }

            Path localFilePath = new Path(localPath);
            Path hdfsFilePath = new Path(hdfsPath);

            // 2. 创建 HDFS 目标目录（如果不存在）
            Path parentPath = hdfsFilePath.getParent();
            if (parentPath != null && !fileSystem.exists(parentPath)) {
                fileSystem.mkdirs(parentPath);
                logger.info("创建目标目录: {}", parentPath);
            }

            // 3. 执行文件上传
            fileSystem.copyFromLocalFile(false, overwrite, localFilePath, hdfsFilePath);
            logger.info("文件上传成功: {} -> {}", localPath, hdfsPath);
            return true;

        } catch (IOException e) {
            logger.error("文件上传失败: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 从 HDFS 下载文件到本地
     * @param hdfsPath HDFS 文件路径
     * @param localPath 本地目标路径
     * @param overwrite 是否覆盖已存在的文件
     * @return 下载是否成功
     */
    public boolean downloadFile(String hdfsPath, String localPath, boolean overwrite) {
        try {
            Path hdfsFilePath = new Path(hdfsPath);
            Path localFilePath = new Path(localPath);

            // 检查 HDFS 文件是否存在
            if (!fileSystem.exists(hdfsFilePath)) {
                logger.error("HDFS 文件不存在: {}", hdfsPath);
                return false;
            }

            // 检查本地文件是否存在
            File localFile = new File(localPath);
            if (localFile.exists() && !overwrite) {
                logger.error("本地文件已存在且不允许覆盖: {}", localPath);
                return false;
            }

            // 执行文件下载
            fileSystem.copyToLocalFile(false, hdfsFilePath, localFilePath, true);
            logger.info("文件下载成功: {} -> {}", hdfsPath, localPath);
            return true;

        } catch (IOException e) {
            logger.error("文件下载失败: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 删除 HDFS 中的文件或目录
     * @param hdfsPath HDFS 路径
     * @param recursive 是否递归删除（用于目录）
     * @return 删除是否成功
     */
    public boolean deleteFile(String hdfsPath, boolean recursive) {
        try {
            Path path = new Path(hdfsPath);
            
            if (!fileSystem.exists(path)) {
                logger.error("路径不存在: {}", hdfsPath);
                return false;
            }

            boolean result = fileSystem.delete(path, recursive);
            if (result) {
                logger.info("删除成功: {}", hdfsPath);
            } else {
                logger.error("删除失败: {}", hdfsPath);
            }
            return result;

        } catch (IOException e) {
            logger.error("删除操作失败: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 递归列出目录中的所有文件和子目录
     * @param hdfsPath HDFS 目录路径
     * @param depth 当前递归深度（用于格式化输出）
     */
    public void listDirectory(String hdfsPath, int depth) {
        try {
            Path path = new Path(hdfsPath);
            
            // 1. 检查路径是否存在
            if (!fileSystem.exists(path)) {
                logger.error("路径不存在: {}", hdfsPath);
                return;
            }

            // 2. 获取目录内容
            FileStatus[] fileStatuses = fileSystem.listStatus(path);
            
            if (fileStatuses == null || fileStatuses.length == 0) {
                System.out.println(getIndent(depth) + "[空目录]");
                return;
            }

            // 3. 递归处理子目录
            for (FileStatus status : fileStatuses) {
                String indent = getIndent(depth);
                String fileName = status.getPath().getName();
                
                if (status.isDirectory()) {
                    System.out.println(indent + "[DIR]  " + fileName);
                    listDirectory(status.getPath().toString(), depth + 1);
                } else {
                    long fileSize = status.getLen();
                    String sizeStr = formatFileSize(fileSize);
                    System.out.println(indent + "[FILE] " + fileName + " (" + sizeStr + ")");
                }
            }

        } catch (IOException e) {
            logger.error("列出目录失败: {}", e.getMessage());
        }
    }

    /**
     * 统计目录信息
     * @param hdfsPath HDFS 目录路径
     * @return DirectoryStats 对象，包含统计信息
     */
    public DirectoryStats getDirectoryStats(String hdfsPath) {
        try {
            Path path = new Path(hdfsPath);
            
            if (!fileSystem.exists(path)) {
                logger.error("路径不存在: {}", hdfsPath);
                return null;
            }

            long fileCount = 0;
            long directoryCount = 0;
            long totalSize = 0;

            // 递归统计
            ContentSummary summary = fileSystem.getContentSummary(path);
            fileCount = summary.getFileCount();
            directoryCount = summary.getDirectoryCount();
            totalSize = summary.getLength();

            return new DirectoryStats(fileCount, directoryCount, totalSize);

        } catch (IOException e) {
            logger.error("统计目录信息失败: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 关闭 HDFS 连接
     */
    public void close() {
        try {
            if (fileSystem != null) {
                fileSystem.close();
                logger.info("HDFS 连接已关闭");
            }
        } catch (IOException e) {
            logger.error("关闭 HDFS 连接失败: {}", e.getMessage());
        }
    }

    /**
     * 获取缩进字符串
     */
    private String getIndent(int depth) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            sb.append("  ");
        }
        return sb.toString();
    }

    /**
     * 格式化文件大小
     */
    private String formatFileSize(long size) {
        if (size < 1024) {
            return size + " B";
        } else if (size < 1024 * 1024) {
            return String.format("%.2f KB", size / 1024.0);
        } else if (size < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", size / (1024.0 * 1024));
        } else {
            return String.format("%.2f GB", size / (1024.0 * 1024 * 1024));
        }
    }

    /**
     * 目录统计信息类
     */
    public static class DirectoryStats {
        private long fileCount;
        private long directoryCount;
        private long totalSize;

        public DirectoryStats(long fileCount, long directoryCount, long totalSize) {
            this.fileCount = fileCount;
            this.directoryCount = directoryCount;
            this.totalSize = totalSize;
        }

        public long getFileCount() {
            return fileCount;
        }

        public long getDirectoryCount() {
            return directoryCount;
        }

        public long getTotalSize() {
            return totalSize;
        }

        @Override
        public String toString() {
            return String.format(
                "目录统计信息:\n" +
                "  文件数量: %d\n" +
                "  目录数量: %d\n" +
                "  总大小: %s",
                fileCount,
                directoryCount,
                formatSize(totalSize)
            );
        }

        private String formatSize(long size) {
            if (size < 1024) {
                return size + " B";
            } else if (size < 1024 * 1024) {
                return String.format("%.2f KB", size / 1024.0);
            } else if (size < 1024 * 1024 * 1024) {
                return String.format("%.2f MB", size / (1024.0 * 1024));
            } else {
                return String.format("%.2f GB", size / (1024.0 * 1024 * 1024));
            }
        }
    }

    /**
     * 主方法，提供命令行交互式界面
     */
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        HDFSFileManager manager = null;
        
        try {
            // 获取 HDFS URI
            System.out.print("请输入 HDFS URI (例如 hdfs://localhost:9000): ");
            String hdfsUri = scanner.nextLine().trim();
            
            if (hdfsUri.isEmpty()) {
                hdfsUri = "hdfs://localhost:9000";
                System.out.println("使用默认 URI: " + hdfsUri);
            }

            // 初始化连接
            manager = new HDFSFileManager(hdfsUri);
            System.out.println("\n=== HDFS 文件管理器已启动 ===");
            System.out.println("成功连接到 HDFS 服务器: " + hdfsUri);
            System.out.println("连接用户: " + System.getProperty("user.name"));
            System.out.println();

            boolean running = true;
            while (running) {
                printMenu();
                System.out.print("请选择操作 (0-6): ");
                String choice = scanner.nextLine().trim();

                switch (choice) {
                    case "1":
                        handleUpload(scanner, manager);
                        break;
                    case "2":
                        handleDownload(scanner, manager);
                        break;
                    case "3":
                        handleDelete(scanner, manager);
                        break;
                    case "4":
                        handleListDirectory(scanner, manager);
                        break;
                    case "5":
                        handleStats(scanner, manager);
                        break;
                    case "6":
                        handleAutoTest(manager);
                        break;
                    case "0":
                        running = false;
                        System.out.println("正在退出...");
                        break;
                    default:
                        System.out.println("无效的选择，请重新输入！");
                }
                
                if (running) {
                    System.out.println("\n按回车键继续...");
                    scanner.nextLine();
                }
            }

        } catch (Exception e) {
            logger.error("程序执行出错", e);
            System.err.println("错误: " + e.getMessage());
        } finally {
            if (manager != null) {
                manager.close();
            }
            scanner.close();
        }
    }

    /**
     * 打印菜单
     */
    private static void printMenu() {
        System.out.println("\n" + new String(new char[50]).replace("\0", "="));
        System.out.println("           HDFS 文件管理器菜单");
        System.out.println(new String(new char[50]).replace("\0", "="));
        System.out.println("1. 上传文件到 HDFS");
        System.out.println("2. 从 HDFS 下载文件");
        System.out.println("3. 删除 HDFS 文件/目录");
        System.out.println("4. 列出目录内容");
        System.out.println("5. 统计目录信息");
        System.out.println("6. 运行自动测试");
        System.out.println("0. 退出程序");
        System.out.println(new String(new char[50]).replace("\0", "="));
    }

    /**
     * 处理文件上传
     */
    private static void handleUpload(Scanner scanner, HDFSFileManager manager) {
        System.out.println("\n========== 文件上传 ==========");
        
        // 使用默认测试文件
        String localPath = "default_test_file.txt";
        File localFile = new File(localPath);
        System.out.println("本地文件路径: " + localFile.getAbsolutePath());
        System.out.println("文件名称: " + localFile.getName());
        if (localFile.exists()) {
            long fileSize = localFile.length();
            String sizeStr = fileSize < 1024 ? fileSize + " B" : 
                            fileSize < 1024 * 1024 ? String.format("%.2f KB", fileSize / 1024.0) :
                            String.format("%.2f MB", fileSize / (1024.0 * 1024));
            System.out.println("文件大小: " + sizeStr);
        }
        
    // 使用默认HDFS路径
    String hdfsPath = "/user/student/project/input/default_test_file.txt";
    System.out.println("HDFS 目标路径: " + hdfsPath);
        
    // 默认覆盖
    boolean overwrite = true;
    System.out.println("覆盖模式: " + (overwrite ? "是（如果文件存在将被覆盖）" : "否"));
        
    System.out.println("\n开始上传...");
    boolean result = manager.uploadFile(localPath, hdfsPath, overwrite);
    System.out.println(result ? "上传成功！" : "上传失败！");
        System.out.println("==============================\n");
    }

    /**
     * 处理文件下载
     */
    private static void handleDownload(Scanner scanner, HDFSFileManager manager) {
        System.out.println("\n========== 文件下载 ==========");
        
    // 使用默认HDFS路径
    String hdfsPath = "/user/student/project/input/default_test_file.txt";
    System.out.println("HDFS 源路径: " + hdfsPath);
        
    // 使用默认本地路径
    String localPath = "downloaded_file.txt";
    File localFile = new File(localPath);
    System.out.println("本地目标路径: " + localFile.getAbsolutePath());
    System.out.println("文件名称: " + localFile.getName());
        
    // 默认覆盖
    boolean overwrite = true;
    System.out.println("覆盖模式: " + (overwrite ? "是（如果文件存在将被覆盖）" : "否"));
        
    System.out.println("\n开始下载...");
    boolean result = manager.downloadFile(hdfsPath, localPath, overwrite);
    System.out.println(result ? "下载成功！" : "下载失败！");
        
            if (result && localFile.exists()) {
            long fileSize = localFile.length();
            String sizeStr = fileSize < 1024 ? fileSize + " B" : 
                            fileSize < 1024 * 1024 ? String.format("%.2f KB", fileSize / 1024.0) :
                            String.format("%.2f MB", fileSize / (1024.0 * 1024));
            System.out.println("下载文件大小: " + sizeStr);
        }
        System.out.println("==============================\n");
    }

    /**
     * 处理文件删除
     */
    private static void handleDelete(Scanner scanner, HDFSFileManager manager) {
        System.out.println("\n========== 文件删除 ==========");
        
    // 使用默认HDFS路径
    String hdfsPath = "/user/student/project/input/default_test_file.txt";
    System.out.println("HDFS 路径: " + hdfsPath);
    System.out.println("操作类型: 删除文件");
        
        // 默认不递归（删除文件）
        boolean recursive = false;
    System.out.println("递归删除: " + (recursive ? "是（删除目录及其内容）" : "否（仅删除文件）"));
        
    System.out.println("\n开始删除...");
    boolean result = manager.deleteFile(hdfsPath, recursive);
    System.out.println(result ? "删除成功！" : "删除失败！");
        System.out.println("==============================\n");
    }

    /**
     * 处理目录列表
     */
    private static void handleListDirectory(Scanner scanner, HDFSFileManager manager) {
        System.out.println("\n========== 列出目录内容 ==========");
        
    // 使用默认HDFS路径
    String hdfsPath = "/user/student/project/input";
    System.out.println("HDFS 目录路径: " + hdfsPath);
        
    System.out.println("\n目录结构:");
        System.out.println("----------------------------");
        manager.listDirectory(hdfsPath, 0);
        System.out.println("----------------------------");
        System.out.println("==============================\n");
    }

    /**
     * 处理目录统计
     */
    private static void handleStats(Scanner scanner, HDFSFileManager manager) {
        System.out.println("\n========== 统计目录信息 ==========");
        
        // 使用默认HDFS路径
        String hdfsPath = "/user/student/project/input";
        System.out.println("HDFS 目录路径: " + hdfsPath);
        
        System.out.println("\n正在统计...");
        DirectoryStats stats = manager.getDirectoryStats(hdfsPath);
        if (stats != null) {
            System.out.println("\n" + stats.toString());
        } else {
            System.out.println("无法获取目录统计信息，可能是目录不存在或无权访问");
        }
        System.out.println("==============================\n");
    }

    /**
     * 运行自动测试
     */
    private static void handleAutoTest(HDFSFileManager manager) {
        System.out.println("\n" + new String(new char[60]).replace("\0", "="));
        System.out.println("           自动测试开始");
        System.out.println(new String(new char[60]).replace("\0", "="));
        
        try {
            // 测试目录
            String testDir = "/user/student/project/test_" + System.currentTimeMillis();
                System.out.println("\n测试目录: " + testDir);
            
            // 1. 创建测试文件
            System.out.println("\n" + new String(new char[60]).replace("\0", "-"));
            System.out.println("【步骤 1/6】创建本地测试文件");
            System.out.println(new String(new char[60]).replace("\0", "-"));
            
            File localTestFile = new File("test_upload.txt");
                System.out.println("本地文件路径: " + localTestFile.getAbsolutePath());
                System.out.println("文件内容: 100行自动生成的测试数据");
            
            try (PrintWriter writer = new PrintWriter(localTestFile)) {
                for (int i = 1; i <= 100; i++) {
                    writer.println("自动测试 - 行号: " + i + " - 这是自动生成的测试内容");
                }
            }
            
            long fileSize = localTestFile.length();
            String sizeStr = fileSize < 1024 ? fileSize + " B" : 
                            fileSize < 1024 * 1024 ? String.format("%.2f KB", fileSize / 1024.0) :
                            String.format("%.2f MB", fileSize / (1024.0 * 1024));
                System.out.println("文件大小: " + sizeStr);
                System.out.println("本地测试文件创建成功");
            
            // 2. 测试上传
            System.out.println("\n" + new String(new char[60]).replace("\0", "-"));
            System.out.println("【步骤 2/6】测试文件上传");
            System.out.println(new String(new char[60]).replace("\0", "-"));
            
            String hdfsTestFile = testDir + "/test_upload.txt";
                System.out.println("本地源文件: " + localTestFile.getAbsolutePath());
                System.out.println("HDFS 目标路径: " + hdfsTestFile);
                System.out.println("覆盖模式: 是");
                System.out.println("\n正在上传...");
            
            boolean uploadResult = manager.uploadFile(localTestFile.getAbsolutePath(), hdfsTestFile, true);
                System.out.println("上传结果: " + (uploadResult ? "成功" : "失败"));
            
            // 3. 测试目录遍历
            System.out.println("\n" + new String(new char[60]).replace("\0", "-"));
            System.out.println("【步骤 3/6】测试目录遍历");
            System.out.println(new String(new char[60]).replace("\0", "-"));
            
                System.out.println("目录路径: " + testDir);
                System.out.println("\n目录结构:");
            manager.listDirectory(testDir, 0);
            
            // 4. 测试目录统计
            System.out.println("\n" + new String(new char[60]).replace("\0", "-"));
            System.out.println("【步骤 4/6】测试目录统计");
            System.out.println(new String(new char[60]).replace("\0", "-"));
            
            System.out.println("统计目录: " + testDir);
            DirectoryStats stats = manager.getDirectoryStats(testDir);
            if (stats != null) {
                System.out.println("\n" + stats.toString());
            }
            
            // 5. 测试下载
            System.out.println("\n" + new String(new char[60]).replace("\0", "-"));
            System.out.println("【步骤 5/6】测试文件下载");
            System.out.println(new String(new char[60]).replace("\0", "-"));
            
            File downloadFile = new File("test_download.txt");
            System.out.println("HDFS 源路径: " + hdfsTestFile);
            System.out.println("本地目标路径: " + downloadFile.getAbsolutePath());
            System.out.println("覆盖模式: 是");
            System.out.println("\n正在下载...");
            
            boolean downloadResult = manager.downloadFile(hdfsTestFile, downloadFile.getAbsolutePath(), true);
            System.out.println("下载结果: " + (downloadResult ? "成功" : "失败"));
            
            if (downloadResult && downloadFile.exists()) {
                long dlSize = downloadFile.length();
                String dlSizeStr = dlSize < 1024 ? dlSize + " B" : 
                                dlSize < 1024 * 1024 ? String.format("%.2f KB", dlSize / 1024.0) :
                                String.format("%.2f MB", dlSize / (1024.0 * 1024));
                System.out.println("下载文件大小: " + dlSizeStr);
            }
            
            // 6. 测试删除
            System.out.println("\n" + new String(new char[60]).replace("\0", "-"));
            System.out.println("【步骤 6/6】测试文件删除");
            System.out.println(new String(new char[60]).replace("\0", "-"));
            
            System.out.println("删除路径: " + testDir);
            System.out.println("递归删除: 是（删除目录及所有内容）");
            System.out.println("\n正在删除...");
            
            boolean deleteResult = manager.deleteFile(testDir, true);
            System.out.println("删除结果: " + (deleteResult ? "成功" : "失败"));
            
            // 清理本地文件
            System.out.println("\n清理本地临时文件...");
            if (localTestFile.delete()) {
                System.out.println("已删除: " + localTestFile.getName());
            }
            if (downloadFile.delete()) {
                System.out.println("已删除: " + downloadFile.getName());
            }
            
            // 测试总结
            System.out.println("\n" + new String(new char[60]).replace("\0", "="));
            System.out.println("           自动测试完成");
            System.out.println(new String(new char[60]).replace("\0", "="));
            System.out.println("所有测试已成功完成！");
            System.out.println();
            
        } catch (Exception e) {
            System.err.println("自动测试失败: " + e.getMessage());
            logger.error("自动测试失败", e);
        }
    }
}
