import React from 'react'
import { Card, Collapse, Empty, List, Space, Tag, Typography } from 'antd'

const { Paragraph, Text } = Typography

interface CamelRoundSummary {
  round: number
  summary?: string
  conflict_count?: number
  consensus?: {
    confidence?: number
    consensus_level?: number
    leading_root_cause?: string
  }
}

interface CamelResultLike {
  collaboration_result?: {
    round_summaries?: Array<CamelRoundSummary | Record<string, unknown>>
    conflict_matrix?: Array<{ root_cause?: string; supporters?: string[]; severity?: string }>
    final_decision?: { root_cause?: string; summary?: string; actions?: string[] }
  }
}

interface Props {
  latestCamel: CamelResultLike | null
}

const RoundReviewPanel: React.FC<Props> = ({ latestCamel }) => {
  const roundSummaries = (latestCamel?.collaboration_result?.round_summaries ?? []).map((item, index) => {
    const round = Number((item as CamelRoundSummary).round ?? (item as Record<string, unknown>).round ?? index + 1)
    return {
      round,
      summary: String((item as CamelRoundSummary).summary ?? (item as Record<string, unknown>).summary ?? ''),
      conflict_count: Number((item as CamelRoundSummary).conflict_count ?? (item as Record<string, unknown>).conflict_count ?? 0),
      consensus: ((item as CamelRoundSummary).consensus ?? (item as Record<string, unknown>).consensus ?? {}) as CamelRoundSummary['consensus'],
    }
  })
  const conflictMatrix = latestCamel?.collaboration_result?.conflict_matrix ?? []
  const finalDecision = latestCamel?.collaboration_result?.final_decision

  return (
    <Card title="CAMEL 轮次回放" className="studio-card">
      {roundSummaries.length ? (
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <Collapse
            items={roundSummaries.map((round) => ({
              key: String(round.round),
              label: `第 ${round.round} 轮`,
              children: (
                <Space direction="vertical" size={10} style={{ width: '100%' }}>
                  <Paragraph className="compact-paragraph">{round.summary || '暂无轮次摘要。'}</Paragraph>
                  <Space wrap>
                    <Tag color="purple">置信度 {Math.round(Number(round.consensus?.confidence || 0) * 100)}%</Tag>
                    <Tag color="gold">共识度 {Math.round(Number(round.consensus?.consensus_level || 0) * 100)}%</Tag>
                    <Tag>{round.consensus?.leading_root_cause || '暂无主因'}</Tag>
                    <Tag>冲突数 {round.conflict_count || 0}</Tag>
                  </Space>
                </Space>
              ),
            }))}
          />

          <List
            size="small"
            header="冲突矩阵"
            dataSource={conflictMatrix}
            locale={{ emptyText: '当前没有未收敛冲突' }}
            renderItem={(item) => (
              <List.Item>
                <Space direction="vertical" size={4}>
                  <Text strong>{item.root_cause || '未命名根因'}</Text>
                  <Text type="secondary">支持者：{item.supporters?.join('、') || '无'}</Text>
                  <Tag color={item.severity === 'high' ? 'red' : item.severity === 'medium' ? 'orange' : 'default'}>
                    冲突等级：{item.severity || 'low'}
                  </Tag>
                </Space>
              </List.Item>
            )}
          />

          {finalDecision ? (
            <div>
              <Text strong>最终裁决</Text>
              <Paragraph className="compact-paragraph">{finalDecision.summary || finalDecision.root_cause}</Paragraph>
            </div>
          ) : null}
        </Space>
      ) : (
        <Empty description="暂无 CAMEL 轮次结果" />
      )}
    </Card>
  )
}

export default RoundReviewPanel
