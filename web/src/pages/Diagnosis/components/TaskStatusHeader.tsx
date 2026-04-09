import React from 'react'
import { Alert, Card, Col, Row, Statistic, Tag, Typography } from 'antd'
import { ClusterOutlined } from '@ant-design/icons'
import type { DiagnosisResultV2, DiagnosisTaskResponseV2 } from '../../../lib/api'

const { Paragraph } = Typography

interface CamelResultLike {
  collaboration_result?: {
    consensus_summary?: { consensus_level?: number; confidence?: number; summary?: string }
  }
}

interface Props {
  currentTask: DiagnosisTaskResponseV2 | null
  taskMessage?: string
  latestResult: DiagnosisResultV2 | null
  latestCamel: CamelResultLike | null
}

const percent = (value?: number) => Math.round((value || 0) * 100)

const statusLabel = (value?: string) => {
  const labels: Record<string, string> = {
    queued: '排队中',
    pending: '排队中',
    running: '运行中',
    completed: '已完成',
    failed: '失败',
    timeout: '超时',
    interrupted: '中断',
    recovered: '已恢复',
  }
  return labels[value || ''] || value || '空闲'
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
    coordinator_started: '协调者整合开始',
    coordinator_completed: '协调者整合完成',
    debate_started: 'CAMEL 协作开始',
    debate_round_started: '协作轮次开始',
    debate_round_completed: '协作轮次完成',
    debate_completed: 'CAMEL 协作完成',
    diagnosis_completed: '诊断完成',
  }
  return labels[value || ''] || value || '任务排队中'
}

const TaskStatusHeader: React.FC<Props> = ({ currentTask, taskMessage, latestResult, latestCamel }) => {
  const workflow = currentTask?.workflow
  const consensusLevel = latestResult?.consensus_level ?? latestCamel?.collaboration_result?.consensus_summary?.consensus_level
  const confidence = latestResult?.confidence ?? latestCamel?.collaboration_result?.consensus_summary?.confidence

  return (
    <div style={{ marginBottom: 16 }}>
      {taskMessage ? <Alert type="info" showIcon style={{ marginBottom: 16 }} message={taskMessage} /> : null}
      <Row gutter={[16, 16]}>
        <Col xs={24} md={8}>
          <Card>
            <Statistic title="当前任务状态" value={statusLabel(workflow?.status || currentTask?.status)} prefix={<ClusterOutlined />} />
            {currentTask ? <Tag style={{ marginTop: 12 }}>{stageLabel(workflow?.current_stage || String((currentTask.progress as { current_action?: string })?.current_action || 'queued'))}</Tag> : null}
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card>
            <Statistic title="共识度" suffix="%" value={percent(consensusLevel)} />
            <Paragraph type="secondary" style={{ marginTop: 12 }}>
              当前结果会随着专家推理和协调者裁决持续刷新。
            </Paragraph>
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card>
            <Statistic title="置信度" suffix="%" value={percent(confidence)} />
            <Paragraph type="secondary" style={{ marginTop: 12 }}>
              当模型不稳定时，系统会自动标记回退并保留可追踪信息。
            </Paragraph>
          </Card>
        </Col>
      </Row>
    </div>
  )
}

export default TaskStatusHeader
