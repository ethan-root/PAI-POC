# POC: GitHub Action 控制阿里云 Flink 部署测试

## 📁 目录结构

```
poc/
├── README.md                           # 本说明文件
├── pom.xml                             # Maven 构建配置
├── src/main/
│   ├── java/.../HelloWorldFlinkJob.java    # Flink 作业代码
│   └── deploy/
│       └── job-args-dev.yml            # Flink 部署配置
└── .github/workflows/
    └── deploy_flink_poc.yml            # GitHub Action 工作流
```

## 🚀 使用步骤

### 1. 填写配置参数

在以下文件中填写标记为 `TODO` 的参数：

| 文件 | 需要填写的参数 |
|------|---------------|
| `pom.xml` | groupId, artifactId |
| `job-args-dev.yml` | 入口类全限定名 |
| `.github/workflows/deploy_flink_poc.yml` | OSS_BUCKET, WORKSPACE, NAMESPACE 等 |

### 2. 配置 GitHub Secrets

在 GitHub 仓库设置中添加以下 Secrets：
- `FLINK_ACCESS_KEY` - 阿里云 AccessKey ID
- `FLINK_ACCESS_SECRET` - 阿里云 AccessKey Secret
- `ARTIFACTORY_USERNAME` - JFrog 用户名
- `ARTIFACTORY_PASSWORD` - JFrog 密码

### 3. 提交代码

```bash
git add .
git commit -m "poc: test github action to aliyun flink deployment"
git push origin main
```

### 4. 观察结果

1. 进入 GitHub Actions 页面
2. 查看工作流执行情况
3. 登录阿里云 Flink 控制台验证部署

## ⚠️ 注意事项

- 确保阿里云 Flink Serverless 服务已开通
- 确保 OSS Bucket 已创建且有写入权限
- 首次测试建议使用最小资源配置
