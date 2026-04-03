import React, { useEffect, useMemo, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { Alert, Button, Card, Col, Empty, Input, InputNumber, List, Row, Select, Space, Spin, Switch, Tag, Typography, message } from 'antd'
import { ApiOutlined, ExperimentOutlined, RobotOutlined, ThunderboltOutlined } from '@ant-design/icons'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  diagnosisV2Api,
  devicesApi,
  extractApiError,
  type AgentCatalogItem,
  type DiagnosisAnalyzeResponseV2,
  type DiagnosisExpertOpinion,
  type DiagnosisResultV2,
  type DiagnosisTaskResponseV2,
  type DiagnosisTaskTraceEvent,
  type ModelProbeResponse,
} from '../../lib/api'
import AgentConfigPanel from './components/AgentConfigPanel'
import ExecutionTimeline from './components/ExecutionTimeline'
import ExpertOpinionBoard from './components/ExpertOpinionBoard'
import ResultSummaryPanel from './components/ResultSummaryPanel'
import RoundReviewPanel from './components/RoundReviewPanel'
import TaskQueuePanel from './components/TaskQueuePanel'
import TaskStatusHeader from './components/TaskStatusHeader'
import TopologyView from './components/TopologyView'
import './style.css'

const { Title, Paragraph, Text } = Typography
const { TextArea } = Input

interface CamelOpinion {
  agent_name: string
  route_key: string
  model_name?: string | null
  used_fallback?: boolean
  duration_ms?: number
  output: {
    root_cause: string
    confidence: number
    evidence?: string[]
    actions?: string[]
    summary?: string
  }
}

interface CamelResult {
  diagnosis_id: string
  symptoms: string
  collaboration_result: {
    mode: string
    rounds: number
    message_count: number
    opinions?: CamelOpinion[]
    round_summaries?: Array<Record<string, unknown>>
    conflict_matrix?: Array<Record<string, unknown>>
    final_decision?: { root_cause?: string; summary?: string; actions?: string[] }
    consensus_summary?: { summary?: string; participants?: number; confidence?: number; consensus_level?: number }
    degraded_mode?: boolean
  }
  expert_count: number
  society: string
}

const DEFAULT_SYMPTOMS = '曝气池溶解氧持续偏低，风机噪声异常，怀疑曝气效率下降。'
const LONG_TASK_POLL_INTERVAL_MS = 5000
const LONG_TASK_POLL_MAX_ATTEMPTS = 900
const LONG_TASK_POLL_WINDOW_MINUTES = Math.round((LONG_TASK_POLL_INTERVAL_MS * LONG_TASK_POLL_MAX_ATTEMPTS) / 60000)

const stageLabel = (value?: string) => {
  const labels: Record<string, string> = {
    queued: '排队中',
    pending: '排队中',
    diagnosis_started: '诊断启动',
    graph_rag_started: 'GraphRAG 检索开始',
    graph_rag_completed: 'GraphRAG 检索完成',
    graph_rag_failed: 'GraphRAG 检索失败',
    expert_started: '专家分析开始',
    expert_completed: '专家分析完成',
    coordinator_started: '协调者整合开始',
    coordinator_completed: '协调者整合完成',
    debate_started: 'CAMEL 协作开始',
    debate_agent_started: '辩论智能体开始分析',
    debate_agent_completed: '辩论智能体完成分析',
    debate_round_started: '协作轮次开始',
    debate_round_completed: '协作轮次完成',
    debate_completed: 'CAMEL 协作完成',
    diagnosis_completed: '诊断完成',
  }
  return labels[value || ''] || value || '任务执行中'
}

const Diagnosis: React.FC = () => {
  const [searchParams] = useSearchParams()
  const taskIdFromUrl = searchParams.get('taskId')
  const [symptoms, setSymptoms] = useState(DEFAULT_SYMPTOMS)
  const [selectedDeviceId, setSelectedDeviceId] = useState<string>()
  const [useGraphRag, setUseGraphRag] = useState(true)
  const [useCamel, setUseCamel] = useState(false)
  const [debug, setDebug] = useState(true)
  const [sensorData, setSensorData] = useState({ do: 1.5, vibration: 8.5, current: 25.3 })
  const [latestResult, setLatestResult] = useState<DiagnosisResultV2 | null>(null)
  const [latestCamel, setLatestCamel] = useState<CamelResult | null>(null)
  const [probeResult, setProbeResult] = useState<ModelProbeResponse | null>(null)
  const [currentTask, setCurrentTask] = useState<DiagnosisTaskResponseV2 | null>(null)
  const [taskMessage, setTaskMessage] = useState<string>()
  const taskStreamRef = useRef<{ close: () => void } | null>(null)

  const devicesQuery = useQuery({ queryKey: ['devices', 'options'], queryFn: devicesApi.list })
  const expertsQuery = useQuery({ queryKey: ['diagnosis-v2', 'experts'], queryFn: diagnosisV2Api.getExperts })
  const tasksQuery = useQuery({
    queryKey: ['diagnosis-v2', 'tasks', currentTask?.task_id ?? 'idle'],
    queryFn: () => diagnosisV2Api.listTasks(10),
    refetchInterval: currentTask && ['pending', 'running'].includes(currentTask.workflow?.status || currentTask.status) ? 15000 : false,
  })

  const runtimeCards = useMemo(() => {
    const experts = expertsQuery.data?.experts ?? []
    const coordinator = expertsQuery.data?.coordinator ? [expertsQuery.data.coordinator] : []
    return [...experts, ...coordinator] as AgentCatalogItem[]
  }, [expertsQuery.data])

  const executionTrace = ((currentTask?.metadata?.execution_trace as DiagnosisTaskTraceEvent[] | undefined) ?? latestResult?.debug?.execution_trace ?? [])
  const runtimeInactive = runtimeCards.length > 0 && runtimeCards.every((item) => !item.runtime?.llm_enabled)

  const latestOpinions: DiagnosisExpertOpinion[] =
    latestResult?.expert_opinions ??
    (latestCamel?.collaboration_result?.opinions ?? []).map((item) => ({
      expert_type: item.route_key,
      expert_name: item.agent_name,
      confidence: item.output.confidence,
      root_cause: item.output.root_cause,
      evidence: item.output.evidence || [],
      suggestions: item.output.actions || [],
      reasoning: item.output.summary || '',
      model_name: item.model_name,
      used_fallback: item.used_fallback,
      duration_ms: item.duration_ms,
      timestamp: new Date().toISOString(),
    }))

  const probeMutation = useMutation({
    mutationFn: diagnosisV2Api.probeModels,
    onSuccess: (response) => {
      setProbeResult(response)
      message.success(response.success ? '模型探测全部成功' : '模型探测已完成，存在部分失败')
    },
    onError: (error) => message.error(extractApiError(error, '模型探测失败')),
  })

  const exportReport = async (taskId: string, format: 'html' | 'pdf' | 'markdown' | 'json') => {
    try {
      const response = await diagnosisV2Api.exportReport(taskId, format)
      message.success(`报告已生成：${response.filename}`)
    } catch (error) {
      message.error(extractApiError(error, '报告导出失败'))
    }
  }

  const hydrateTaskResult = (task: DiagnosisTaskResponseV2) => {
    setCurrentTask(task)
    if (task.status === 'completed' && task.result) {
      const resultType = String(task.metadata?.final_result_type || '')
      if (resultType === 'camel' || 'collaboration_result' in task.result) {
        setLatestCamel(task.result as unknown as CamelResult)
        setLatestResult(null)
      } else {
        setLatestResult(task.result as unknown as DiagnosisResultV2)
        setLatestCamel(null)
      }
      setTaskMessage(undefined)
      return true
    }
    return false
  }

  const stopTaskStreaming = () => {
    taskStreamRef.current?.close()
    taskStreamRef.current = null
  }

  const updateTaskMessageFromTask = (task: DiagnosisTaskResponseV2, trackingMode: 'stream' | 'poll') => {
    const action = stageLabel(task.workflow?.current_stage || String((task.progress as { current_action?: string })?.current_action || 'pending'))
    const percentage = Number((task.progress as { percentage?: number })?.percentage || 0)
    const restored = task.recovery?.restored_from_persistence ? '，已从持久化状态恢复' : ''
    const trackingLabel = trackingMode === 'stream' ? '实时追踪中' : '轮询追踪中'
    setTaskMessage(`${trackingLabel}：${action}，进度 ${Math.round(percentage)}%，任务 ${task.task_id}${restored}，最长等待窗口约 ${LONG_TASK_POLL_WINDOW_MINUTES} 分钟。`)
  }

  const pollLongTask = async (taskId: string) => {
    for (let attempt = 0; attempt < LONG_TASK_POLL_MAX_ATTEMPTS; attempt += 1) {
      const task = await diagnosisV2Api.getTask(taskId)
      if (hydrateTaskResult(task)) return

      if (['failed', 'timeout', 'cancelled'].includes(task.status) || task.workflow?.status === 'interrupted') {
        throw new Error(task.error || '诊断任务失败')
      }

      updateTaskMessageFromTask(task, 'poll')
      await new Promise((resolve) => setTimeout(resolve, LONG_TASK_POLL_INTERVAL_MS))
    }
    throw new Error(`诊断任务等待超过约 ${LONG_TASK_POLL_WINDOW_MINUTES} 分钟`)
  }

  const trackTaskWithRealtime = async (taskId: string) =>
    new Promise<void>((resolve, reject) => {
      stopTaskStreaming()
      let settled = false
      let sawSnapshot = false

      const settle = (callback: () => void) => {
        if (settled) return
        settled = true
        stopTaskStreaming()
        callback()
      }

      const fallbackToPolling = (reason?: string) => {
        if (settled) return
        stopTaskStreaming()
        if (reason) {
          setTaskMessage(`${reason}，已切换为轮询追踪。`)
        }
        void pollLongTask(taskId)
          .then(() => settle(resolve))
          .catch((error) => settle(() => reject(error instanceof Error ? error : new Error('诊断任务追踪失败'))))
      }

      taskStreamRef.current = diagnosisV2Api.streamTask(taskId, {
        onSnapshot: (task) => {
          sawSnapshot = true
          if (hydrateTaskResult(task)) {
            settle(resolve)
            return
          }
          if (['failed', 'timeout', 'cancelled'].includes(task.status) || task.workflow?.status === 'interrupted') {
            settle(() => reject(new Error(task.error || '诊断任务失败')))
            return
          }
          updateTaskMessageFromTask(task, 'stream')
        },
        onComplete: () => {
          if (settled) return
          void diagnosisV2Api
            .getTask(taskId)
            .then((task) => {
              if (hydrateTaskResult(task)) {
                settle(resolve)
                return
              }
              if (['failed', 'timeout', 'cancelled'].includes(task.status) || task.workflow?.status === 'interrupted') {
                settle(() => reject(new Error(task.error || '诊断任务失败')))
                return
              }
              fallbackToPolling('实时流已结束')
            })
            .catch(() => fallbackToPolling('实时流结束后获取任务快照失败'))
        },
        onError: (error) => {
          if (settled) return
          const reason = sawSnapshot ? `实时连接中断：${error.message}` : `实时订阅不可用：${error.message}`
          fallbackToPolling(reason)
        },
      })
    })

  const diagnoseMutation = useMutation({
    mutationFn: diagnosisV2Api.analyze,
    onSuccess: async (response: DiagnosisAnalyzeResponseV2) => {
      setLatestCamel(null)
      setLatestResult(null)
      if (response.status === 'processing' && response.task_id) {
        setCurrentTask({
          task_id: response.task_id,
          status: 'pending',
          progress: { current_action: response.message, percentage: 0 },
        })
        message.loading({ content: `任务已启动，最长追踪窗口约 ${LONG_TASK_POLL_WINDOW_MINUTES} 分钟...`, key: 'diagnosis-poll' })
        try {
          await trackTaskWithRealtime(response.task_id)
          message.success({ content: useCamel ? 'CAMEL 协作诊断完成' : '多智能体诊断完成', key: 'diagnosis-poll' })
        } catch (error) {
          message.error({ content: extractApiError(error, '任务追踪失败'), key: 'diagnosis-poll' })
        }
        return
      }
      setLatestResult(response.result ?? null)
      setCurrentTask(null)
      setTaskMessage(undefined)
      message.success('诊断完成')
    },
    onError: (error) => message.error(extractApiError(error, '诊断请求失败')),
  })

  const handleDiagnose = () => {
    if (!symptoms.trim()) {
      message.warning('请先输入故障现象')
      return
    }
    setLatestResult(null)
    setLatestCamel(null)
    setCurrentTask(null)
    diagnoseMutation.mutate({
      symptoms,
      device_id: selectedDeviceId,
      sensor_data: sensorData,
      use_multi_agent: true,
      use_graph_rag: useGraphRag,
      use_camel: useCamel,
      debug,
      priority: 'normal',
    })
  }

  const handleAttachTask = async (taskId: string) => {
    try {
      setLatestCamel(null)
      setLatestResult(null)
      const task = await diagnosisV2Api.getTask(taskId)
      if (hydrateTaskResult(task)) {
        message.success(`已加载任务 ${taskId} 的结果`)
        return
      }
      message.info(`已接管任务 ${taskId}`)
      await trackTaskWithRealtime(taskId)
    } catch (error) {
      message.error(extractApiError(error, '接管任务失败'))
    }
  }

  useEffect(() => {
    if (!taskIdFromUrl) return
    if (currentTask?.task_id === taskIdFromUrl) return
    void handleAttachTask(taskIdFromUrl)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [taskIdFromUrl])

  useEffect(() => () => stopTaskStreaming(), [])

  return (
    <div className="diagnosis-studio">
      <div className="diagnosis-hero">
        <div>
          <Tag color="blue">商用多智能体诊断</Tag>
          <Title level={2}>诊断运营控制台</Title>
          <Paragraph>
            这个页面会完整展示诊断引擎背后的真实工作流，包括任务持久化、模型绑定、GraphRAG、专家推理、CAMEL 多轮协作和报告导出。
          </Paragraph>
        </div>
        <Space wrap>
          <Button icon={<ExperimentOutlined />} onClick={() => probeMutation.mutate()} loading={probeMutation.isPending}>探测真实模型</Button>
          <Button type="primary" icon={<ThunderboltOutlined />} onClick={handleDiagnose} loading={diagnoseMutation.isPending}>运行诊断</Button>
        </Space>
      </div>

      <TaskStatusHeader currentTask={currentTask} taskMessage={taskMessage} latestResult={latestResult} latestCamel={latestCamel} />

      <Row gutter={[16, 16]}>
        <Col xs={24} xl={8}>
          <Card title={<Space><RobotOutlined />诊断输入</Space>} className="studio-card">
            <Space direction="vertical" size="middle" style={{ width: '100%' }}>
              <Select
                allowClear
                placeholder="可选：关联设备"
                value={selectedDeviceId}
                onChange={(value) => setSelectedDeviceId(value)}
                loading={devicesQuery.isLoading}
                options={(devicesQuery.data?.devices ?? []).map((device) => ({ label: device.name, value: device.id }))}
              />
              <TextArea value={symptoms} onChange={(e) => setSymptoms(e.target.value)} autoSize={{ minRows: 5, maxRows: 8 }} />
              <Row gutter={12}>
                <Col span={8}>
                  <Text type="secondary">DO</Text>
                  <InputNumber min={0} max={20} step={0.1} value={sensorData.do} onChange={(value) => setSensorData((prev) => ({ ...prev, do: Number(value ?? 0) }))} style={{ width: '100%' }} />
                </Col>
                <Col span={8}>
                  <Text type="secondary">振动</Text>
                  <InputNumber min={0} max={50} step={0.1} value={sensorData.vibration} onChange={(value) => setSensorData((prev) => ({ ...prev, vibration: Number(value ?? 0) }))} style={{ width: '100%' }} />
                </Col>
                <Col span={8}>
                  <Text type="secondary">电流</Text>
                  <InputNumber min={0} max={100} step={0.1} value={sensorData.current} onChange={(value) => setSensorData((prev) => ({ ...prev, current: Number(value ?? 0) }))} style={{ width: '100%' }} />
                </Col>
              </Row>
              <div className="switch-grid">
                <label><span>GraphRAG</span><Switch checked={useGraphRag} onChange={setUseGraphRag} /></label>
                <label><span>CAMEL 协作</span><Switch checked={useCamel} onChange={setUseCamel} /></label>
                <label><span>调试信息</span><Switch checked={debug} onChange={setDebug} /></label>
              </div>
              <Alert type="info" showIcon message="长任务窗口已开启" description={`控制台现在会持续追踪长任务诊断，最长约 ${LONG_TASK_POLL_WINDOW_MINUTES} 分钟。`} />
              {taskMessage ? <Alert type="info" showIcon message={taskMessage} /> : null}
            </Space>
          </Card>

          <Card title={<Space><ApiOutlined />模型探测</Space>} className="studio-card">
            {probeMutation.isPending ? <Spin /> : null}
            {!probeResult && !probeMutation.isPending ? <Empty description="点击按钮后会探测真实模型端点" /> : null}
            {probeResult ? (
              <List
                itemLayout="vertical"
                dataSource={probeResult.probes}
                renderItem={(item) => (
                  <List.Item>
                    <Space direction="vertical" size={4} style={{ width: '100%' }}>
                      <Space wrap>
                        <Tag color={item.success ? 'success' : 'error'}>{item.route_key}</Tag>
                        <Tag>{item.model_name || '未绑定'}</Tag>
                        <Tag>{item.endpoint || '无端点'}</Tag>
                        {item.latency_ms ? <Tag>{item.latency_ms} ms</Tag> : null}
                      </Space>
                      <Text type={item.success ? undefined : 'danger'}>{item.success ? item.response_excerpt : item.error}</Text>
                    </Space>
                  </List.Item>
                )}
              />
            ) : null}
          </Card>

          <TaskQueuePanel
            tasks={tasksQuery.data?.tasks ?? []}
            loading={tasksQuery.isLoading}
            onAttach={(taskId) => void handleAttachTask(taskId)}
            onExport={(taskId, format) => void exportReport(taskId, format)}
          />
        </Col>

        <Col xs={24} xl={16}>
          {runtimeInactive ? (
            <Alert
              type="warning"
              showIcon
              style={{ marginBottom: 16 }}
              message="当前后端运行时尚未加载多模型路由"
              description="页面展示的是 /v2/diagnosis/experts 返回的真实运行态。如果你预期这里已经绑定模型，请确认页面当前连接的是哪一个后端进程。"
            />
          ) : null}

          <AgentConfigPanel agents={runtimeCards} />
          <TopologyView useGraphRag={useGraphRag} agents={runtimeCards} latestOpinions={latestOpinions} executionTrace={executionTrace} />
          <ExecutionTimeline executionTrace={executionTrace} />
          <RoundReviewPanel latestCamel={latestCamel} />
          <ResultSummaryPanel latestResult={latestResult} latestCamel={latestCamel} currentTaskId={currentTask?.task_id} onExport={(taskId, format) => void exportReport(taskId, format)} />
          <ExpertOpinionBoard opinions={latestOpinions} />
        </Col>
      </Row>
    </div>
  )
}

export default Diagnosis
