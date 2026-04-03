import React from 'react'
import { Card, Space, Tag, Timeline, Typography } from 'antd'
import type { DiagnosisTaskTraceEvent } from '../../../lib/api'

const { Text } = Typography

interface Props {
  executionTrace: DiagnosisTaskTraceEvent[]
}

const stageLabel = (value?: string) => {
  const labels: Record<string, string> = {
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
  return labels[value || ''] || value || '执行中'
}

const ExecutionTimeline: React.FC<Props> = ({ executionTrace }) => {
  const currentRound = executionTrace.reduce((max, event) => Math.max(max, Number(event.round || 0)), 0)
  const completedExpertCount = new Set(
    executionTrace
      .filter((event) => event.stage === 'expert_completed' || event.stage === 'debate_agent_completed')
      .map((event) => event.agent_key || event.agent_id || event.agent_name),
  ).size
  const latestDurationMs = [...executionTrace].reverse().find((event) => typeof event.duration_ms === 'number')?.duration_ms

  return (
    <Card title="执行时间线" className="studio-card">
      <Space wrap style={{ marginBottom: 12 }}>
        <Tag color="blue">当前轮次 {currentRound}</Tag>
        <Tag color="cyan">已完成智能体 {completedExpertCount}</Tag>
        {typeof latestDurationMs === 'number' ? <Tag color="purple">最新阶段 {Math.round(latestDurationMs)} ms</Tag> : null}
      </Space>
      <Timeline
        items={executionTrace.map((event) => ({
          color: event.stage.includes('failed') ? 'red' : event.stage.includes('completed') ? 'green' : 'blue',
          children: (
            <Space direction="vertical" size={2}>
              <Text strong>{stageLabel(event.stage)}</Text>
              <Text type="secondary">
                {new Date(event.timestamp).toLocaleString()}
                {event.round ? ` · 轮次 ${event.round}` : ''}
                {typeof event.progress === 'number' ? ` · ${Math.round(event.progress)}%` : ''}
                {typeof event.duration_ms === 'number' ? ` · ${Math.round(event.duration_ms)} ms` : ''}
              </Text>
            </Space>
          ),
        }))}
      />
    </Card>
  )
}

export default ExecutionTimeline
