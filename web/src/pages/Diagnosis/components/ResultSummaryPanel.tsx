import React from 'react'
import { Button, Card, Empty, List, Space, Tag, Typography } from 'antd'
import type { DiagnosisResultV2 } from '../../../lib/api'

const { Paragraph } = Typography

interface CamelResultLike {
  collaboration_result?: {
    consensus_summary?: { summary?: string; confidence?: number; consensus_level?: number }
    final_decision?: { summary?: string; root_cause?: string; actions?: string[] }
    degraded_mode?: boolean
  }
}

interface Props {
  latestResult: DiagnosisResultV2 | null
  latestCamel: CamelResultLike | null
  currentTaskId?: string
  onExport: (taskId: string, format: 'html' | 'pdf' | 'markdown' | 'json') => void
}

const ResultSummaryPanel: React.FC<Props> = ({ latestResult, latestCamel, currentTaskId, onExport }) => {
  const multiAgent = latestResult
  const camel = latestCamel?.collaboration_result
  const title = multiAgent ? multiAgent.final_conclusion : camel?.final_decision?.summary || camel?.final_decision?.root_cause

  return (
    <Card
      title="结果摘要"
      className="studio-card"
      extra={
        currentTaskId ? (
          <Space>
            <Button size="small" onClick={() => onExport(currentTaskId, 'html')}>
              HTML 报告
            </Button>
            <Button size="small" onClick={() => onExport(currentTaskId, 'json')}>
              JSON 报告
            </Button>
          </Space>
        ) : null
      }
    >
      {title ? (
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <Paragraph className="compact-paragraph">{title}</Paragraph>
          {multiAgent ? (
            <>
              <Space wrap>
                <Tag color="purple">置信度 {Math.round(multiAgent.confidence * 100)}%</Tag>
                <Tag color="gold">共识度 {Math.round(multiAgent.consensus_level * 100)}%</Tag>
                {multiAgent.related_cases.map((item) => (
                  <Tag key={item}>{item}</Tag>
                ))}
              </Space>
              <List
                size="small"
                header="推荐动作"
                dataSource={multiAgent.recommended_actions}
                renderItem={(item) => <List.Item>{item.action}</List.Item>}
              />
            </>
          ) : (
            <>
              <Space wrap>
                <Tag color="purple">置信度 {Math.round(Number(camel?.consensus_summary?.confidence || 0) * 100)}%</Tag>
                <Tag color="gold">共识度 {Math.round(Number(camel?.consensus_summary?.consensus_level || 0) * 100)}%</Tag>
                {camel?.degraded_mode ? <Tag color="orange">降级模式</Tag> : null}
              </Space>
              <List
                size="small"
                header="协调者动作"
                dataSource={camel?.final_decision?.actions || []}
                renderItem={(item) => <List.Item>{item}</List.Item>}
              />
            </>
          )}
        </Space>
      ) : (
        <Empty description="暂无诊断结果" />
      )}
    </Card>
  )
}

export default ResultSummaryPanel
