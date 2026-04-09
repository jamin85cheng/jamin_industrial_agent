import React, { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Alert, Button, Card, Col, Empty, List, Progress, Row, Space, Statistic, Table, Tag, Typography, message } from 'antd'
import {
  ApiOutlined,
  BellOutlined,
  ClockCircleOutlined,
  DashboardOutlined,
  DatabaseOutlined,
  RobotOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons'
import { useMutation, useQuery } from '@tanstack/react-query'
import type { TableProps } from 'antd'
import {
  alertsApi,
  devicesApi,
  diagnosisV2Api,
  extractApiError,
  reportsApi,
  type AlertRecord,
  type DeviceRecord,
  type DiagnosisResultV2,
  type DiagnosisTaskResponseV2,
} from '../../lib/api'
import './style.css'

const { Paragraph, Text, Title } = Typography

type DeviceStatus = 'online' | 'warning' | 'offline'

interface DashboardDevice {
  id: string
  name: string
  status: DeviceStatus
  tagCount: number
  uptime: number
}

const severityMeta: Record<AlertRecord['severity'], { color: string; label: string }> = {
  critical: { color: 'red', label: '严重' },
  warning: { color: 'orange', label: '警告' },
  info: { color: 'blue', label: '提示' },
}

const statusMeta: Record<DeviceStatus, { color: string; label: string }> = {
  online: { color: 'success', label: '在线' },
  warning: { color: 'warning', label: '关注' },
  offline: { color: 'error', label: '离线' },
}

const workflowColor = (status?: string) => {
  if (status === 'completed') return 'success'
  if (status === 'failed' || status === 'timeout' || status === 'interrupted') return 'error'
  if (status === 'recovered') return 'gold'
  return 'processing'
}

const workflowLabel = (status?: string) => {
  const labels: Record<string, string> = {
    queued: '排队中',
    pending: '排队中',
    running: '运行中',
    completed: '已完成',
    failed: '失败',
    timeout: '超时',
    interrupted: '中断',
    recovered: '已恢复',
    cancelled: '已取消',
  }
  return labels[status || ''] || status || '未知'
}

const stageLabel = (value?: string) => {
  const labels: Record<string, string> = {
    queued: '排队中',
    diagnosis_started: '诊断启动',
    graph_rag_started: 'GraphRAG 检索开始',
    graph_rag_completed: 'GraphRAG 检索完成',
    graph_rag_failed: 'GraphRAG 检索失败',
    expert_started: '专家分析开始',
    expert_completed: '专家分析完成',
    coordinator_started: '协调器整合开始',
    coordinator_completed: '协调器整合完成',
    debate_started: 'CAMEL 协作开始',
    debate_round_started: '协作轮次开始',
    debate_round_completed: '协作轮次完成',
    debate_completed: 'CAMEL 协作完成',
    diagnosis_completed: '诊断完成',
  }
  return labels[value || ''] || value || '排队中'
}

const normalizeDeviceStatus = (status: DeviceRecord['status']): DeviceStatus => {
  if (status === 'error') return 'warning'
  return status
}

const buildUptime = (status: DeviceStatus) => {
  if (status === 'online') return 98
  if (status === 'warning') return 86
  return 63
}

const isTaskActive = (task: DiagnosisTaskResponseV2) => {
  const workflowStatus = task.workflow?.status || task.status
  return ['queued', 'pending', 'running', 'recovered'].includes(workflowStatus)
}

const mergeTaskSnapshot = (tasks: DiagnosisTaskResponseV2[], nextTask: DiagnosisTaskResponseV2) => {
  const next = [...tasks]
  const index = next.findIndex((task) => task.task_id === nextTask.task_id)
  if (index >= 0) {
    next[index] = nextTask
  } else {
    next.unshift(nextTask)
  }
  return next.slice(0, 12)
}

const DeviceCard: React.FC<{ device: DashboardDevice }> = ({ device }) => (
  <Card className="device-card" size="small">
    <div className="device-card-content">
      <div className="device-info">
        <div className="device-name">{device.name}</div>
        <div className="device-tags">
          {device.tagCount} 个测点
          <span className="device-dot" />
          <Tag color={statusMeta[device.status].color}>{statusMeta[device.status].label}</Tag>
        </div>
      </div>
    </div>
    <Progress percent={device.uptime} size="small" strokeColor={device.status === 'offline' ? '#c33b2d' : '#246bce'} style={{ marginTop: 12 }} />
  </Card>
)

const AlertTable: React.FC<{
  alerts: AlertRecord[]
  loading: boolean
  diagnosing?: boolean
  onDiagnose?: (alertId: string) => void
  onOpenTask?: (taskId: string) => void
  onDownloadReport?: (reportId: string) => void
}> = ({ alerts, loading, diagnosing = false, onDiagnose, onOpenTask, onDownloadReport }) => {
  const columns: TableProps<AlertRecord>['columns'] = [
    {
      title: '级别',
      dataIndex: 'severity',
      key: 'severity',
      width: 96,
      render: (severity: AlertRecord['severity']) => <Tag color={severityMeta[severity].color}>{severityMeta[severity].label}</Tag>,
    },
    { title: '规则', dataIndex: 'rule_name', key: 'rule_name', render: (value: string | null | undefined) => value || '-' },
    { title: '描述', dataIndex: 'message', key: 'message', ellipsis: true },
    { title: '时间', dataIndex: 'created_at', key: 'created_at', width: 180, render: (value: string) => new Date(value).toLocaleString() },
    {
      title: '操作',
      key: 'action',
      width: 280,
      render: (_, record) => (
        <Space wrap>
          {onDiagnose ? (
            <Button size="small" loading={diagnosing} onClick={() => onDiagnose(record.id)}>
              发起诊断
            </Button>
          ) : null}
          {record.diagnosis_task_id && onOpenTask ? (
            <Button size="small" type="link" onClick={() => onOpenTask(record.diagnosis_task_id!)}>
              查看任务
            </Button>
          ) : null}
          {record.latest_report_id && onDownloadReport ? (
            <Button size="small" type="link" onClick={() => onDownloadReport(record.latest_report_id!)}>
              下载报告
            </Button>
          ) : null}
        </Space>
      ),
    },
  ]

  return (
    <Table
      columns={columns}
      dataSource={alerts}
      rowKey="id"
      loading={loading}
      locale={{ emptyText: <Empty description="暂无告警" /> }}
      size="small"
      pagination={false}
      scroll={{ y: 220 }}
    />
  )
}

const Dashboard: React.FC = () => {
  const navigate = useNavigate()
  const [liveTasks, setLiveTasks] = useState<DiagnosisTaskResponseV2[]>([])

  const devicesQuery = useQuery({ queryKey: ['devices'], queryFn: devicesApi.list })
  const alertsQuery = useQuery({ queryKey: ['alerts', 'recent'], queryFn: alertsApi.list })
  const alertStatsQuery = useQuery({ queryKey: ['alerts', 'stats'], queryFn: alertsApi.stats })
  const expertsQuery = useQuery({ queryKey: ['diagnosis-v2', 'experts', 'dashboard'], queryFn: diagnosisV2Api.getExperts })
  const historyQuery = useQuery({ queryKey: ['diagnosis-v2', 'history', 'dashboard'], queryFn: () => diagnosisV2Api.getHistory(5) })
  const tasksQuery = useQuery({
    queryKey: ['diagnosis-v2', 'tasks', 'dashboard'],
    queryFn: () => diagnosisV2Api.listTasks(12),
    refetchInterval: 15000,
  })
  const runtimeDebugQuery = useQuery({
    queryKey: ['diagnosis-v2', 'runtime-debug', 'dashboard'],
    queryFn: diagnosisV2Api.getRuntimeDebug,
    refetchInterval: 30000,
  })

  const diagnoseAlertMutation = useMutation({
    mutationFn: (alertId: string) =>
      diagnosisV2Api.analyzeAlert(alertId, {
        use_graph_rag: true,
        use_camel: false,
        debug: true,
        priority: 'high',
      }),
    onSuccess: (response, alertId) => {
      if (!response.task_id) {
        message.warning(`告警 ${alertId} 已触发诊断，但未返回任务编号`)
        return
      }
      message.success(`已从告警 ${alertId} 发起诊断`)
      navigate(`/diagnosis?taskId=${encodeURIComponent(response.task_id)}`)
    },
    onError: (error) => {
      message.error(extractApiError(error, '从告警发起诊断失败'))
    },
  })

  useEffect(() => {
    setLiveTasks(tasksQuery.data?.tasks ?? [])
  }, [tasksQuery.data])

  const activeTaskIds = useMemo(
    () =>
      liveTasks
        .filter(isTaskActive)
        .slice(0, 4)
        .map((task) => task.task_id),
    [liveTasks],
  )

  useEffect(() => {
    if (!activeTaskIds.length) return

    const streams = activeTaskIds.map((taskId) =>
      diagnosisV2Api.streamTask(taskId, {
        onSnapshot: (task) => {
          setLiveTasks((prev) => mergeTaskSnapshot(prev, task))
        },
        onComplete: () => {
          void tasksQuery.refetch()
          void runtimeDebugQuery.refetch()
        },
        onError: () => {
          void tasksQuery.refetch()
        },
      }),
    )

    return () => {
      streams.forEach((stream) => stream.close())
    }
  }, [activeTaskIds, tasksQuery, runtimeDebugQuery])

  const handleDownloadReport = async (reportId: string) => {
    try {
      await reportsApi.download(reportId)
      message.success('报告下载已开始')
    } catch (error) {
      message.error(extractApiError(error, '下载报告失败'))
    }
  }

  const deviceItems: DashboardDevice[] = (devicesQuery.data?.devices ?? []).map((device) => {
    const status = normalizeDeviceStatus(device.status)
    return { id: device.id, name: device.name, status, tagCount: device.tag_count, uptime: buildUptime(status) }
  })

  const recentAlerts = (alertsQuery.data?.alerts ?? []).slice(0, 5)
  const recentDiagnosis = historyQuery.data?.history?.[0] as DiagnosisResultV2 | undefined
  const runtimeCards = [...(expertsQuery.data?.experts ?? []), ...(expertsQuery.data?.coordinator ? [expertsQuery.data.coordinator] : [])]
  const llmEnabledCount = runtimeCards.filter((item) => Boolean(item.runtime?.llm_enabled)).length
  const runtimeInactive = runtimeCards.length > 0 && llmEnabledCount === 0
  const taskFeed = liveTasks.length ? liveTasks : tasksQuery.data?.tasks ?? []
  const diagnosisExecutor = runtimeDebugQuery.data?.runtime.diagnosis_executor
  const diagnosisBootstrap = runtimeDebugQuery.data?.runtime.diagnosis_runtime_bootstrap
  const diagnosisExecutionBackend =
    diagnosisExecutor?.backend || runtimeDebugQuery.data?.config.diagnosis_execution_backend || 'background_tasks'
  const diagnosisQueueDepth = diagnosisExecutor?.queue_depth ?? runtimeDebugQuery.data?.runtime.task_tracker?.total_queued ?? 0
  const diagnosisWorkerCount =
    diagnosisExecutor?.worker_count ??
    diagnosisExecutor?.max_workers ??
    runtimeDebugQuery.data?.config.diagnosis_execution_asyncio_workers ??
    0

  const taskSummary = useMemo(() => {
    return {
      running: taskFeed.filter((task) => ['queued', 'pending', 'running'].includes(task.workflow?.status || task.status)).length,
      recovered: taskFeed.filter((task) => task.workflow?.status === 'recovered').length,
      interrupted: taskFeed.filter((task) => task.workflow?.status === 'interrupted').length,
      completed: taskFeed.filter((task) => (task.workflow?.status || task.status) === 'completed').length,
      failed: taskFeed.filter((task) => ['failed', 'timeout'].includes(task.workflow?.status || task.status)).length,
    }
  }, [taskFeed])

  return (
    <div className="dashboard-page">
      {(devicesQuery.isError || alertsQuery.isError || alertStatsQuery.isError || expertsQuery.isError || historyQuery.isError || tasksQuery.isError || runtimeDebugQuery.isError) ? (
        <Alert type="warning" showIcon style={{ marginBottom: 16 }} message="部分运营数据加载失败" description="首页会优先展示当前已经成功获取的数据。" />
      ) : null}

      <section className="dashboard-hero">
        <div className="dashboard-hero-copy">
          <Tag color="cyan">企业级诊断工作台</Tag>
          <Title level={2}>工业诊断运营总览</Title>
          <Paragraph>
            这里把告警、诊断任务、报告交付和运行时状态放到同一张首页里，便于运维和值班同学快速判断链路是否已经跑通。
          </Paragraph>
          <Space wrap>
            <Button type="primary" icon={<ThunderboltOutlined />} onClick={() => navigate('/diagnosis')}>
              进入诊断控制台
            </Button>
            <Button icon={<RobotOutlined />} onClick={() => void runtimeDebugQuery.refetch()}>
              刷新运行态
            </Button>
          </Space>
        </div>
        <div className="dashboard-signal-board">
          <div className="signal-pill"><span>智能体在线</span><strong>{runtimeCards.length}</strong></div>
          <div className="signal-pill"><span>LLM 路由</span><strong>{llmEnabledCount}</strong></div>
          <div className="signal-pill"><span>运行中任务</span><strong>{taskSummary.running}</strong></div>
          <div className="signal-pill"><span>Queue depth</span><strong>{diagnosisQueueDepth}</strong></div>
          <div className="signal-pill"><span>Workers</span><strong>{diagnosisWorkerCount}</strong></div>
          <div className="signal-pill"><span>Executor</span><strong>{String(diagnosisExecutionBackend)}</strong></div>
        </div>
      </section>

      {runtimeInactive ? (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
          message="当前后端进程尚未激活多模型路由"
          description="这通常意味着当前连接的实例还没有加载生产模型配置，可以去诊断控制台进一步核查。"
        />
      ) : null}

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} sm={12} lg={6}><Card><Statistic title="设备总数" value={deviceItems.length} prefix={<DashboardOutlined />} /></Card></Col>
        <Col xs={24} sm={12} lg={6}><Card><Statistic title="测点总数" value={deviceItems.reduce((sum, item) => sum + item.tagCount, 0)} prefix={<DatabaseOutlined />} /></Card></Col>
        <Col xs={24} sm={12} lg={6}><Card><Statistic title="活动告警" value={alertStatsQuery.data?.active_alerts ?? 0} prefix={<BellOutlined />} valueStyle={{ color: '#c33b2d' }} /></Card></Col>
        <Col xs={24} sm={12} lg={6}><Card><Statistic title="AI 智能体" value={runtimeCards.length} prefix={<ApiOutlined />} /></Card></Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} xl={10}>
          <Card title="最近诊断摘要" className="dashboard-card" loading={historyQuery.isLoading}>
            {recentDiagnosis ? (
              <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                <Space wrap>
                  <Tag color="geekblue">{new Date(recentDiagnosis.generated_at).toLocaleString()}</Tag>
                  <Tag color="purple">置信度 {Math.round(recentDiagnosis.confidence * 100)}%</Tag>
                  <Tag color="gold">共识度 {Math.round(recentDiagnosis.consensus_level * 100)}%</Tag>
                </Space>
                <Paragraph style={{ marginBottom: 0 }}>{recentDiagnosis.final_conclusion}</Paragraph>
                <Space wrap>
                  {(recentDiagnosis.related_cases ?? []).slice(0, 3).map((item) => <Tag key={item}>{item}</Tag>)}
                </Space>
              </Space>
            ) : (
              <Empty description="暂无诊断历史" />
            )}
          </Card>
        </Col>

        <Col xs={24} xl={14}>
          <Card title="诊断运行态" className="dashboard-card" loading={runtimeDebugQuery.isLoading}>
            <div className="task-summary-grid">
              <div className="task-summary-card"><span>任务存储</span><strong>{String(runtimeDebugQuery.data?.runtime.task_tracker?.storage || 'unknown')}</strong></div>
              <div className="task-summary-card"><span>元数据存储</span><strong>{String(runtimeDebugQuery.data?.runtime.metadata_database?.backend || 'unknown')}</strong></div>
              <div className="task-summary-card"><span>活动任务</span><strong>{runtimeDebugQuery.data?.runtime.task_tracker?.active_tasks ?? taskSummary.running}</strong></div>
              <div className="task-summary-card"><span>运行任务</span><strong>{runtimeDebugQuery.data?.runtime.task_tracker?.running_tasks ?? 0}</strong></div>
              <div className="task-summary-card"><span>排队任务</span><strong>{diagnosisQueueDepth}</strong></div>
              <div className="task-summary-card"><span>自动恢复</span><strong>{diagnosisBootstrap?.auto_resume_enabled ? 'On' : 'Off'}</strong></div>
            </div>
            <Space direction="vertical" size={4} style={{ marginTop: 16, width: '100%' }}>
              <Text type="secondary">任务目标：{runtimeDebugQuery.data?.runtime.task_tracker?.persistence_path || '-'}</Text>
              <Text type="secondary">元数据目标：{runtimeDebugQuery.data?.runtime.metadata_database?.target || '-'}</Text>
              <Text type="secondary">启动恢复数量：{diagnosisBootstrap?.auto_resumed_task_ids?.length ?? 0}</Text>
              <Text type="secondary">启动时间：{diagnosisBootstrap?.bootstrapped_at ? new Date(diagnosisBootstrap.bootstrapped_at).toLocaleString() : '-'}</Text>
              <Space wrap>
                <Tag color={diagnosisExecutor?.durable ? 'success' : 'default'}>{diagnosisExecutor?.durable ? 'Durable execution' : 'Non-durable execution'}</Tag>
                <Tag color={diagnosisExecutor?.requires_worker ? 'processing' : 'default'}>{diagnosisExecutor?.requires_worker ? 'Worker required' : 'In-process'}</Tag>
                {diagnosisExecutor?.resolution_note ? <Tag>{String(diagnosisExecutor.resolution_note)}</Tag> : null}
              </Space>
            </Space>
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} xl={14}>
          <Card title="持久化诊断任务" className="dashboard-card" loading={tasksQuery.isLoading}>
            {taskFeed.length > 0 ? (
              <List
                className="task-list"
                dataSource={taskFeed}
                renderItem={(task: DiagnosisTaskResponseV2) => {
                  const percentage = Math.round(Number((task.progress as { percentage?: number })?.percentage || 0))
                  const currentStage = stageLabel(task.workflow?.current_stage || String((task.progress as { current_action?: string })?.current_action || 'queued'))
                  return (
                    <List.Item
                      actions={[
                        <Button key={task.task_id} type="link" size="small" onClick={() => navigate(`/diagnosis?taskId=${encodeURIComponent(task.task_id)}`)}>
                          接管任务
                        </Button>,
                      ]}
                    >
                      <div className="task-row">
                        <div className="task-row-main">
                          <Space wrap>
                            <Tag color={workflowColor(task.workflow?.status || task.status)}>{workflowLabel(task.workflow?.status || task.status)}</Tag>
                            <Tag>{task.metadata?.diagnosis_mode || task.task_type}</Tag>
                            <Tag icon={<ClockCircleOutlined />}>{task.runtime?.timeout_seconds ?? '-'} 秒预算</Tag>
                            {task.recovery?.restored_from_persistence ? <Tag color="gold">已恢复</Tag> : null}
                          </Space>
                          <div><Text strong>{currentStage}</Text></div>
                          <div>
                            <Text type="secondary">
                              轮次 {task.workflow?.current_round || 0} · 进度 {percentage}% · {task.duration_seconds ? `已运行 ${Math.round(task.duration_seconds)} 秒` : '尚未开始'}
                            </Text>
                          </div>
                          {task.runtime?.target ? <div><Text type="secondary">存储目标：{task.runtime.target}</Text></div> : null}
                        </div>
                      </div>
                    </List.Item>
                  )
                }}
              />
            ) : (
              <Empty description="暂无诊断任务" />
            )}
          </Card>
        </Col>

        <Col xs={24} xl={10}>
          <Card title="设备健康快照" className="dashboard-card" loading={devicesQuery.isLoading}>
            <div className="device-grid">
              {deviceItems.length ? deviceItems.slice(0, 6).map((device) => <DeviceCard key={device.id} device={device} />) : <Empty description="暂无设备" />}
            </div>
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col xs={24}>
          <Card title="最近告警" className="dashboard-card">
            <AlertTable
              alerts={recentAlerts}
              loading={alertsQuery.isLoading}
              diagnosing={diagnoseAlertMutation.isPending}
              onDiagnose={(alertId) => diagnoseAlertMutation.mutate(alertId)}
              onOpenTask={(taskId) => navigate(`/diagnosis?taskId=${encodeURIComponent(taskId)}`)}
              onDownloadReport={(reportId) => void handleDownloadReport(reportId)}
            />
          </Card>
        </Col>
      </Row>
    </div>
  )
}

export default Dashboard
