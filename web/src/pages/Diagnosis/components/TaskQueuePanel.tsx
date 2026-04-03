import React from 'react'
import { Button, Card, Empty, List, Space, Tag, Typography } from 'antd'
import type { DiagnosisTaskResponseV2 } from '../../../lib/api'

const { Paragraph, Text } = Typography

interface Props {
  tasks: DiagnosisTaskResponseV2[]
  loading?: boolean
  onAttach: (taskId: string) => void
  onExport: (taskId: string, format: 'html' | 'pdf' | 'markdown' | 'json') => void
}

const statusLabel = (value?: string) => {
  const labels: Record<string, string> = {
    pending: '排队中',
    running: '运行中',
    completed: '已完成',
    failed: '失败',
    timeout: '超时',
    interrupted: '中断',
    recovered: '已恢复',
  }
  return labels[value || ''] || value || '未知'
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
  return labels[value || ''] || value || '等待启动'
}

const TaskQueuePanel: React.FC<Props> = ({ tasks, loading, onAttach, onExport }) => (
  <Card title="持久化任务队列" className="studio-card" loading={loading}>
    <Paragraph type="secondary" className="compact-paragraph">
      诊断任务会持久化保存，因此运维人员可以重新打开任务、查看恢复状态，并在完成后导出报告。当前卡片会直接显示任务使用的是 Postgres 还是 SQLite。
    </Paragraph>
    {tasks.length ? (
      <List
        size="small"
        dataSource={tasks}
        renderItem={(task) => (
          <List.Item
            actions={[
              <Button key={`attach-${task.task_id}`} type="link" size="small" onClick={() => onAttach(task.task_id)}>
                接管
              </Button>,
              <Button
                key={`report-${task.task_id}`}
                type="link"
                size="small"
                disabled={(task.workflow?.status || task.status) !== 'completed'}
                onClick={() => onExport(task.task_id, 'html')}
              >
                报告
              </Button>,
            ]}
          >
            <Space direction="vertical" size={4} style={{ width: '100%' }}>
              <Space wrap>
                <Tag color="blue">{task.metadata?.diagnosis_mode || task.task_type}</Tag>
                <Tag color={(task.workflow?.status || task.status) === 'completed' ? 'success' : 'processing'}>
                  {statusLabel(task.workflow?.status || task.status)}
                </Tag>
                <Tag color={task.runtime?.storage === 'postgres' ? 'geekblue' : 'default'}>{task.runtime?.storage || 'sqlite'}</Tag>
                {task.recovery?.restored_from_persistence ? <Tag color="gold">已恢复</Tag> : null}
                {task.recovery?.interrupted_by_restart ? <Tag color="red">重启中断</Tag> : null}
              </Space>
              <Text strong>{stageLabel(task.workflow?.current_stage || String((task.progress as { current_action?: string })?.current_action || 'queued'))}</Text>
              <Text type="secondary">
                轮次 {task.workflow?.current_round || 0} · 进度 {Math.round(Number((task.progress as { percentage?: number })?.percentage || 0))}% ·
                超时预算 {task.runtime?.timeout_seconds ?? '-'} 秒
              </Text>
              {task.runtime?.target ? <Text type="secondary">存储目标：{task.runtime.target}</Text> : null}
            </Space>
          </List.Item>
        )}
      />
    ) : (
      <Empty description="暂无任务" />
    )}
  </Card>
)

export default TaskQueuePanel
