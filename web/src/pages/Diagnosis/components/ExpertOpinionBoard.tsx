import React from 'react'
import { Card, Empty, List, Space, Tag, Typography } from 'antd'
import type { DiagnosisExpertOpinion } from '../../../lib/api'

const { Paragraph, Text } = Typography

interface Props {
  opinions: DiagnosisExpertOpinion[]
}

const ExpertOpinionBoard: React.FC<Props> = ({ opinions }) => (
  <Card title="专家意见板" className="studio-card">
    {opinions.length ? (
      <List
        dataSource={opinions}
        renderItem={(item) => (
          <List.Item>
            <Space direction="vertical" size={8} style={{ width: '100%' }}>
              <Space wrap>
                <Tag color="blue">{item.expert_name}</Tag>
                <Tag>{item.model_name || 'heuristic'}</Tag>
                <Tag color="purple">置信度 {Math.round(item.confidence * 100)}%</Tag>
                {item.used_fallback ? <Tag color="orange">回退</Tag> : null}
                {typeof item.duration_ms === 'number' ? <Tag>{Math.round(item.duration_ms)} ms</Tag> : null}
              </Space>
              <Text strong>{item.root_cause}</Text>
              <Paragraph className="compact-paragraph">{item.reasoning}</Paragraph>
              <div>
                <Text type="secondary">证据</Text>
                <ul className="compact-list">{item.evidence.map((evidence) => <li key={evidence}>{evidence}</li>)}</ul>
              </div>
              <div>
                <Text type="secondary">建议动作</Text>
                <ul className="compact-list">{item.suggestions.map((suggestion) => <li key={suggestion}>{suggestion}</li>)}</ul>
              </div>
            </Space>
          </List.Item>
        )}
      />
    ) : (
      <Empty description="暂无专家意见" />
    )}
  </Card>
)

export default ExpertOpinionBoard
