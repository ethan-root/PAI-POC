#!/usr/bin/env python
"""
Flink job deploy scripts
用于将 Flink 作业部署到阿里云 Ververica Platform (VVP) 的 Python 脚本。
主要功能包括：
1. 创建/更新 Deployment
2. 启动/停止 Job
3. 状态检查与等待
4. Savepoint 管理
"""

import argparse
import yaml
import time
import sys
import os
import logging
from typing import List, Dict, Any, Optional

# 导入阿里云 Ververica Platform SDK 相关模块
from alibabacloud_ververica20220718.client import Client as VervericaClient
from alibabacloud_ververica20220718 import models as vvp_models
from alibabacloud_tea_openapi.models import Config
from alibabacloud_tea_util.models import RuntimeOptions

# 导入 VVP 模型类
from alibabacloud_ververica20220718.models import Deployment
from alibabacloud_ververica20220718.models import Artifact
from alibabacloud_ververica20220718.models import BriefDeploymentTarget
from alibabacloud_ververica20220718.models import StreamingResourceSetting
from alibabacloud_ververica20220718.models import BasicResourceSetting
from alibabacloud_ververica20220718.models import Logging
from alibabacloud_ververica20220718.models import JarArtifact
from alibabacloud_ververica20220718.models import BasicResourceSettingSpec
from alibabacloud_ververica20220718.models import LogReservePolicy
from alibabacloud_ververica20220718.models import StartJobWithParamsRequest
from alibabacloud_ververica20220718.models import StartJobWithParamsHeaders
from alibabacloud_ververica20220718.models import JobStartParameters
from alibabacloud_ververica20220718.models import DeploymentRestoreStrategy
from alibabacloud_ververica20220718.models import StopJobRequestBody

# 配置日志格式
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FlinkDeployer:
  """
  Flink 部署器类，封装了与阿里云 VVP 交互的逻辑
  """
  def __init__(self, access_key: str, access_secret: str, region_id: str):
    """
    初始化 VVP 客户端
    :param access_key: 阿里云 Access Key ID
    :param access_secret: 阿里云 Access Key Secret
    :param region_id: 地域 ID (目前代码硬编码了 endpoint 为杭州)
    """
    config = Config(
        access_key_id=access_key,
        access_key_secret=access_secret,
        region_id=region_id,
        endpoint=f'ververica.{region_id}.aliyuncs.com'
    )
    self.client = VervericaClient(config)

  def _check_response_success(self, response):
    """
    检查 API 响应是否成功，失败则打印日志并退出程序
    """
    if hasattr(response, 'body') and hasattr(response.body, 'success') and response.body.success is False:
      logger.error(f"Full response: {response}")
      sys.exit(1)


  def build_deployment_spec(self, args: argparse.Namespace) -> Deployment:
    """
    根据传入参数构建 Deployment 对象（作业配置的核心数据结构）
    """
    # 1. 配置 JobManager 资源 (CPU, Memory)
    jobmanager_resource = BasicResourceSettingSpec(
        cpu=float(args.jobmanager_cpu),
        memory=args.jobmanager_memory
    )

    # 2. 配置 TaskManager 资源 (CPU, Memory)
    taskmanager_resource = BasicResourceSettingSpec(
        cpu=float(args.taskmanager_cpu),
        memory=args.taskmanager_memory
    )

    # 3. 组装基础资源配置 (包含并发度 Parallelism)
    basic_resource_setting = BasicResourceSetting(
        parallelism=int(args.parallelism),
        jobmanager_resource_setting_spec=jobmanager_resource,
        taskmanager_resource_setting_spec=taskmanager_resource
    )

    # 4. 设置为流式作业资源模式 (Basic Mode)
    streaming_resource_setting = StreamingResourceSetting(
        resource_setting_mode='BASIC',
        basic_resource_setting=basic_resource_setting
    )

    # 5. 指定部署目标 (Session Cluster 或 Per Job Cluster，这里配置为 PER_JOB 模式)
    # 并指定部署在 'default-queue' 队列上
    deployment_target = BriefDeploymentTarget(
        mode='PER_JOB',
        name='default-queue'
    )

    # 6. 配置 OpenTelemetry (OTEL) 监控代理
    # 硬编码了 OTEL jar 包和配置文件的 OSS 路径
    opentelemetry_javaagent = f"oss://{args.oss_bucket}/cicd/jars/opentelemetry-javaagent.jar"
    opentelemetry_javaagent_config = f"oss://{args.oss_bucket}/cicd/jars/otel-agent-config.yaml"


    # 将 OTEL 依赖添加到 additional_dependencies 列表中
    if args.additional_dependencies:
      args.additional_dependencies.append(opentelemetry_javaagent)
      args.additional_dependencies.append(opentelemetry_javaagent_config)
    else:
      args.additional_dependencies = [opentelemetry_javaagent, opentelemetry_javaagent_config]

    # 7. 构建 Jar Artifact 信息 (Main Class, Args, Dependencies)
    jar_artifact = JarArtifact(
        jar_uri=args.jar_uri,
        entry_class=args.entry_class,
        main_args=args.main_args,
        additional_dependencies=args.additional_dependencies
    )

    artifact = Artifact(
        kind='JAR',
        jar_artifact=jar_artifact
    )

    # 8. 日志保留策略配置 (保留 7 天)
    log_reserve_policy = LogReservePolicy(
        open_history=True,
        expiration_days=7
    )

    logging_config = Logging(
        logging_profile='default',
        log_reserve_policy=log_reserve_policy
    )

    # 9. 构造 JVM 参数，注入 OTEL Java Agent 配置
    # 分别为 TaskManager 和 JobManager 配置 OTEL 相关的环境变量
    env_java_opts_taskmanager = f"-javaagent:/flink/usrlib/opentelemetry-javaagent.jar -Dotel.resource.attributes=service.name={args.job_name}-taskManager,deployment.environment={args.env} -Dotel.exporter.otlp.traces.endpoint=${{secret_values.ALICLOUD_OTEL_URL}}/adapt_${{secret_values.ALICLOUD_OTEL_TOKEN}}/api/otlp/traces -Dotel.logs.exporter=none -Dotel.metrics.exporter=none -Dotel.experimental.config.file=/flink/usrlib/otel-agent-config.yaml"

    env_java_opts_jobmanager = f"-javaagent:/flink/usrlib/opentelemetry-javaagent.jar -Dotel.resource.attributes=service.name={args.job_name}-jobManager,deployment.environment={args.env} -Dotel.exporter.otlp.traces.endpoint=${{secret_values.ALICLOUD_OTEL_URL}}/adapt_${{secret_values.ALICLOUD_OTEL_TOKEN}}/api/otlp/traces -Dotel.logs.exporter=none -Dotel.metrics.exporter=none -Dotel.experimental.config.file=/flink/usrlib/otel-agent-config.yaml"

    # 10. Flink Configuration (flink-conf.yaml)
    # 包含 Checkpoint, Restart Strategy, State TTL 等关键配置
    flink_conf = {
      'execution.checkpointing.interval': '180s', # Checkpoint 间隔 3分钟
      'restart-strategy': 'fixed-delay', # 重启策略：固定延迟
      'restart-strategy.fixed-delay.attempts': '2147483647', # 重试次数 (Integer.MAX_VALUE)
      'restart-strategy.fixed-delay.delay': '10 s', # 重试延迟 10秒
      'execution.checkpointing.min-pause': '180s',
      'table.exec.state.ttl': '36 h', # State TTL 36小时
      'env.java.opts': '-Dconfig.disable-inline-comment=true',
      'env.java.opts.taskmanager': env_java_opts_taskmanager,
      'env.java.opts.jobmanager': env_java_opts_jobmanager
    }

    # 11. 构建最终的 Deployment 对象
    deployment = Deployment(
        name=args.job_name,
        execution_mode='STREAMING', # 执行模式：流式
        streaming_resource_setting=streaming_resource_setting,
        deployment_target=deployment_target,
        artifact=artifact,
        engine_version=args.engine_version, # VVP 引擎版本
        flink_conf=flink_conf,
        logging=logging_config
    )

    return deployment

  def get_deployment_by_name(self, namespace: str, workspace: str, job_name: str) -> Optional[Dict[str, Any]]:
    """
    根据作业名称查询 Deployment
    """
    try:
      runtime = RuntimeOptions()

      response = self.client.list_deployments_with_options(
          namespace,
          vvp_models.ListDeploymentsRequest(name=job_name),
          vvp_models.ListDeploymentsHeaders(workspace=workspace),
          runtime
      )

      self._check_response_success(response)

      if response.body.data:
        # 遍历返回列表，精确匹配名称
        deployment = response.body.data[0].to_map()
        if deployment.get('name') == job_name:
          return deployment
        else:
          # API 支持模糊搜索，所以需要二次确认名称完全一致
          raise Exception(f"Deployment name mismatch: expected {job_name}, got {deployment.get('name')}")

      return None

    except Exception as e:
      raise e

  def get_latest_job(self, namespace: str, workspace: str, deployment_id: str) -> Optional[Dict[str, Any]]:
    """
    获取指定 Deployment ID 下最新的 Job 实例
    """
    try:
      runtime = RuntimeOptions()
      response = self.client.list_jobs_with_options(
          namespace,
          vvp_models.ListJobsRequest(deployment_id=deployment_id),
          vvp_models.ListJobsHeaders(workspace=workspace),
          runtime
      )

      self._check_response_success(response)

      # 列表默认按创建时间倒序，取第一个即为最新 Job
      if hasattr(response.body.data, '__len__') and len(response.body.data) > 0:
        return response.body.data[0].to_map()
      return None

    except Exception as e:
      raise e

  def create_deployment(self, namespace: str, workspace: str, deployment_spec: Deployment) -> Dict[str, Any]:
    """
    创建新的 Deployment
    """
    try:
      runtime = RuntimeOptions()

      response = self.client.create_deployment_with_options(
          namespace,
          vvp_models.CreateDeploymentRequest(body=deployment_spec),
          vvp_models.CreateDeploymentHeaders(workspace=workspace),
          runtime
      )

      self._check_response_success(response)

      if response:
        return response.body.data.to_map()
      raise Exception("Failed to create deployment: Empty response")

    except Exception as e:
      raise e

  def update_deployment(self, namespace: str, workspace: str, deployment_id: str, deployment_spec: Deployment) -> Dict[str, Any]:
    """
    更新现有的 Deployment 配置
    """
    try:
      # 更新时不需要传 name，防止 ID 和 Name 冲突问题
      deployment_spec.name = None
      deployment_spec.deployment_id = deployment_id

      runtime = RuntimeOptions()
      response = self.client.update_deployment_with_options(
          namespace,
          deployment_id,
          vvp_models.UpdateDeploymentRequest(body=deployment_spec),
          vvp_models.UpdateDeploymentHeaders(workspace=workspace),
          runtime
      )

      self._check_response_success(response)

      if response:
        return response.to_map()
      raise Exception("Failed to update deployment: Empty response")

    except Exception as e:
      raise e

  def start_job_with_params(self, namespace: str, workspace: str, deployment_id: str, restore_strategy: DeploymentRestoreStrategy = None) -> Dict[str, Any]:
    """
    启动作业，支持指定恢复策略 (无状态启动 / 从 Savepoint 启动 / 从最新状态启动)
    """
    try:
      runtime = RuntimeOptions()

      job_start_params = JobStartParameters(
          deployment_id=deployment_id,
          restore_strategy=restore_strategy
      )

      request = StartJobWithParamsRequest(
          body=job_start_params
      )

      headers = StartJobWithParamsHeaders(
          workspace=workspace
      )

      response = self.client.start_job_with_params_with_options(
          namespace,
          request,
          headers,
          runtime
      )

      self._check_response_success(response)

      if response:
        return response.body.data.to_map()
      raise Exception("Failed to start job: Empty response")

    except Exception as e:
      raise e

  def stop_job(self, namespace: str, workspace: str, job_id: str) -> Dict[str, Any]:
    """
    停止作业 (Stop Strategy: NONE，即直接停止不触发 Savepoint，但在本脚本逻辑中，手动触发 Savepoint 在先)
    """
    try:
      runtime = RuntimeOptions()
      stop_job_request_body = StopJobRequestBody(stop_strategy="NONE")
      response = self.client.stop_job_with_options(
          namespace,
          job_id,
          vvp_models.StopJobRequest(body=stop_job_request_body),
          vvp_models.StopJobHeaders(workspace=workspace),
          runtime
      )

      self._check_response_success(response)

      if response:
        return response.to_map()
      raise Exception("Failed to stop job: Empty response")

    except Exception as e:
      raise e

  def get_job_status(self, namespace: str, workspace: str, job_id: str) -> Optional[str]:
    """
    获取 Job 当前状态
    """
    try:
      runtime = RuntimeOptions()
      response = self.client.get_job_with_options(
          namespace,
          job_id,
          vvp_models.GetJobHeaders(workspace=workspace),
          runtime
      )

      self._check_response_success(response)

      if hasattr(response.body, 'data')  and hasattr(response.body.data, 'status'):
        return response.body.data.status.current_job_status

      return None

    except Exception as e:
      raise e

  def create_savepoint(self, namespace: str, workspace: str, deployment_id: str, description: str = "Auto savepoint before update") -> Dict[str, Any]:
    """
    手动触发 Savepoint
    """
    try:
      runtime = RuntimeOptions()
      response = self.client.create_savepoint_with_options(
          namespace,
          vvp_models.CreateSavepointRequest(
              deployment_id=deployment_id,
              description=description
          ),
          vvp_models.CreateSavepointHeaders(workspace=workspace),
          runtime
      )

      self._check_response_success(response)

      if response:
        return response.body.data.to_map()
      raise Exception("Failed to create savepoint: Empty response")

    except Exception as e:
      raise e

  def get_savepoint_status(self, namespace: str, workspace: str, savepoint_id: str) -> Optional[str]:
    """
    获取 Savepoint 状态
    """
    try:
      runtime = RuntimeOptions()
      response = self.client.get_savepoint_with_options(
          namespace,
          savepoint_id,
          vvp_models.GetSavepointHeaders(workspace=workspace),
          runtime
      )

      self._check_response_success(response)

      if hasattr(response.body.data, 'status'):
        return response.body.data.status.state

      return None

    except Exception as e:
      raise e

  def wait_for_job_status(self, namespace: str, workspace: str, job_id: str, target_status: str,
      timeout: int = 600, interval: int = 10) -> bool:
    """
    轮询等待 Job 达到目标状态 (默认超时 600秒)
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
      current_status = self.get_job_status(namespace, workspace, job_id)
      logger.info(f"Current job status: {current_status}")

      if current_status == target_status:
        return True
      elif current_status in ['FAILED', 'CANCELLED', 'FINISHED']:
        raise Exception(f"Job reached terminal state: {current_status}")

      time.sleep(interval)

    raise Exception(f"Timeout waiting for job to reach {target_status}")

  def wait_for_savepoint(self, namespace: str, workspace: str, savepoint_id: str,
      timeout: int = 300, interval: int = 5) -> bool:
    """
    轮询等待 Savepoint 完成 (默认超时 300秒)
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
      status = self.get_savepoint_status(namespace, workspace, savepoint_id)
      logger.info(f"Current savepoint status: {status}")

      if status == 'COMPLETED':
        return True
      elif status in ['FAILED', 'CANCELLED']:
        raise Exception(f"Savepoint failed with status: {status}")

      time.sleep(interval)

    raise Exception("Timeout waiting for savepoint to complete")

  def deploy(self, args: argparse.Namespace) -> bool:
    """
    【核心逻辑】 部署主流程
    """
    try:
      # 1. 构建 Deployment 配置对象
      deployment_spec = self.build_deployment_spec(args)
      namespace = args.namespace
      workspace = args.workspace

      logger.info(f"Starting deployment for job: {args.job_name}")

      # 2. 检查 Deployment 是否已存在
      existing_deployment = self.get_deployment_by_name(namespace, workspace, args.job_name)

      if not existing_deployment:
        # === Case A: 新 Deployment ===
        logger.info("Creating new deployment...")
        deployment_result = self.create_deployment(namespace, workspace, deployment_spec)
        deployment_id = deployment_result.get('deploymentId')

        if not deployment_id:
          raise Exception("Failed to get deployment ID from creation response")

        logger.info(f"Deployment created: {deployment_id}")

        # 直接启动 Job (无状态)
        logger.info("Starting job...")
        restore_strategy = DeploymentRestoreStrategy(
            kind='NONE'
        )
        job_result = self.start_job_with_params(namespace, workspace, deployment_id, restore_strategy)
        job_id = job_result.get('jobId')

        if not job_id:
          raise Exception(f"Failed to get job ID from start response: {job_result}")

        logger.info(f"Job started: {job_id}")

        logger.info(f"Waiting for job:{job_id} to reach RUNNING state...")
        self.wait_for_job_status(namespace, workspace, job_id, 'RUNNING')

        logger.info("✅ Deployment completed successfully!")
        return True

      else:
        # === Case B: 现有 Deployment 更新 ===
        deployment_id = existing_deployment.get('deploymentId')
        logger.info(f"Updating existing deployment: {deployment_id}")

        # 获取该 Deployment 下最新的 Job 实例
        latest_job = self.get_latest_job(namespace, workspace, deployment_id)

        if not latest_job:
          # 如果没有 Job 记录，视为新任务无状态启动
          restore_strategy = DeploymentRestoreStrategy(
              kind='NONE'
          )
        else:
          job_id = latest_job.get('jobId')
          current_status = latest_job.get('status').get('currentJobStatus')

          logger.info(f"Latest job {job_id} status: {current_status}")

          if current_status == 'RUNNING':
            # 如果 Job 正在运行，先做 Savepoint 再停止
            logger.info("Job is RUNNING, creating savepoint...")
            savepoint_result = self.create_savepoint(namespace, workspace, deployment_id)
            savepoint_id = savepoint_result.get('savepointId')

            logger.info(f"Savepoint created: {savepoint_id}")
            logger.info("Waiting for savepoint to complete...")
            self.wait_for_savepoint(namespace, workspace, savepoint_id)

            logger.info("Stopping current job...")
            self.stop_job(namespace, workspace, job_id)

            logger.info("Waiting for job to stop...")
            self.wait_for_job_status(namespace, workspace, job_id, 'CANCELLED') # 等待直到状态变为 CANCELLED

          # 3. 更新 Deployment 配置
          logger.info("Updating deployment configuration...")
          self.update_deployment(namespace, workspace, deployment_id, deployment_spec)

          logger.info("Starting updated job...")
          if current_status == 'RUNNING':
            # 如果之前是运行状态，则从刚才创建的 Savepoint 恢复
            restore_strategy = DeploymentRestoreStrategy(
                kind='FROM_SAVEPOINT',
                savepoint_id=savepoint_id
            )
          else:
            # 否则从最新状态恢复
            restore_strategy = DeploymentRestoreStrategy(
                kind='LATEST_STATE'
            )

        # 4. 启动新 Job
        new_job_result = self.start_job_with_params(namespace, workspace, deployment_id, restore_strategy)
        new_job_id = new_job_result.get('jobId')

        logger.info(f"New job started: {new_job_id}")

        logger.info("Waiting for job to reach RUNNING state...")
        self.wait_for_job_status(namespace, workspace, new_job_id, 'RUNNING')

        logger.info("✅ Deployment updated successfully!")
        return True

    except Exception as e:
      logger.exception(u"❌ Deployment failed: {}".format(e))
      return False


def load_args_from_file(config_path: str) -> argparse.Namespace:
  """
  从 YAML 文件加载参数
  """
  if not os.path.exists(config_path):
    raise FileNotFoundError(f"Config file not found: {config_path}")

  with open(config_path, 'r', encoding='utf-8') as f:
    config_data = yaml.safe_load(f)

  file_args = argparse.Namespace()

  for key, value in config_data.items():
    setattr(file_args, key, value)

  return file_args

def merge_arguments(cmd_args: argparse.Namespace, file_args: argparse.Namespace) -> argparse.Namespace:
  """
  合并命令行参数和文件参数，命令行参数优先级更高
  """
  merged_args = argparse.Namespace()

  arg_fields = [attr for attr in dir(cmd_args) if not attr.startswith('_')]

  for field in arg_fields:
    cmd_value = getattr(cmd_args, field)
    file_value = getattr(file_args, field, None)

    if cmd_value is not None:
      setattr(merged_args, field, cmd_value)
    elif file_value is not None:
      setattr(merged_args, field, file_value)
    else:
      setattr(merged_args, field, None)

  return merged_args

def validate_required_args(args: argparse.Namespace):
  """
  校验必填参数
  """
  required_args = [
    'access_key', 'access_secret', 'workspace', 'namespace',
    'job_name', 'parallelism', 'jobmanager_cpu', 'jobmanager_memory',
    'taskmanager_cpu', 'taskmanager_memory', 'jar_uri',
    'entry_class', 'main_args'
  ]

  missing_args = []
  for arg_name in required_args:
    if not hasattr(args, arg_name) or getattr(args, arg_name) is None:
      missing_args.append(arg_name)

  if missing_args:
    raise ValueError(f"Missing required arguments: {', '.join(missing_args)}")

def main():
  """
  主入口，处理参数解析
  """
  parser = argparse.ArgumentParser(description='Deploy Flink job to VVP')

  parser.add_argument('--args-file', required=False, help='YAML configuration file path')

  parser.add_argument('--access-key', required=True, help='Access Key ID')
  parser.add_argument('--access-secret', required=True, help='Access Key Secret')
  parser.add_argument('--oss-bucket', required=False, help='oss bucket name')
  parser.add_argument('--region', default='cn-hangzhou', help='Region ID')

  parser.add_argument('--workspace', required=True, help='Workspace ID')
  parser.add_argument('--namespace', required=True, help='Namespace')

  parser.add_argument('--job-name', required=False, help='Job name')
  parser.add_argument('--env', required=False, help='Environment')
  parser.add_argument('--engine-version', required=False, default='vvr-8.0.11-jdk11-flink-1.17', help='Flink Engine Version')
  parser.add_argument('--parallelism', required=False, help='Parallelism')
  parser.add_argument('--jobmanager-cpu', required=False, help='JobManager CPU cores')
  parser.add_argument('--jobmanager-memory', required=False, help='JobManager memory (e.g., 4 GiB)')
  parser.add_argument('--taskmanager-cpu', required=False, help='TaskManager CPU cores')
  parser.add_argument('--taskmanager-memory', required=False, help='TaskManager memory (e.g., 4 GiB)')

  parser.add_argument('--jar-uri', required=False, help='JAR file URI')
  parser.add_argument('--entry-class', required=False, help='Main class')
  parser.add_argument('--main-args', required=False, help='Main arguments')

  parser.add_argument('--additional-dependencies', nargs='*',
                      help='Additional dependency files')

  args = parser.parse_args()

  # 如果提供了配置文件，则加载并合并
  if args.args_file:
    file_args = load_args_from_file(args.args_file)
    args = merge_arguments(args, file_args)

  validate_required_args(args)

  deployer = FlinkDeployer(args.access_key, args.access_secret, args.region)

  success = deployer.deploy(args)
  sys.exit(0 if success else 1)


if __name__ == '__main__':
  main()