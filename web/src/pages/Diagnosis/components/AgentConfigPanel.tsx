import React from 'react'
import { Card, Col, Collapse, Row, Space, Tag, Typography } from 'antd'
import type { AgentCatalogItem } from '../../../lib/api'

const { Paragraph, Text } = Typography

interface Props {
  agents: AgentCatalogItem[]
}

const AgentConfigPanel: React.FC<Props> = ({ agents }) => (
  <Card title="智能体配置与模型绑定" className="studio-card">
    <Row gutter={[12, 12]}>
      {agents.map((item) => (
        <Col xs={24} lg={12} key={item.id}>
          <div className="runtime-config-card">
            <Space direction="vertical" size={10} style={{ width: '100%' }}>
              <div className="runtime-header">
                <div>
                  <Text strong>{item.name}</Text>
                  <div>
                    <Text type="secondary">{item.description}</Text>
                  </div>
                </div>
                <Space wrap className="runtime-tags">
                  <Tag color="blue">{String(item.runtime?.model_name || '未绑定')}</Tag>
                  <Tag color={item.runtime?.llm_enabled ? 'success' : 'default'}>
                    {item.runtime?.llm_enabled ? '模型已启用' : '模型未启用'}
                  </Tag>
                </Space>
              </div>
              <Space wrap>
                <Tag>{item.type}</Tag>
                {item.capabilities.map((capability) => (
                  <Tag key={capability}>{capability}</Tag>
                ))}
              </Space>
              <div className="runtime-meta-grid">
                <div>
                  <Text type="secondary">端点</Text>
                  <div>{String(item.runtime?.endpoint || '-')}</div>
                </div>
                <div>
                  <Text type="secondary">超时</Text>
                  <div>{item.runtime?.timeout_seconds ?? '-'} 秒</div>
                </div>
                <div>
                  <Text type="secondary">温度</Text>
                  <div>{item.runtime?.temperature ?? '-'}</div>
                </div>
                <div>
                  <Text type="secondary">最大 Tokens</Text>
                  <div>{item.runtime?.max_tokens ?? '-'}</div>
                </div>
              </div>
              <Collapse
                size="small"
                items={[
                  {
                    key: 'prompt',
                    label: '提示词与输出约束',
                    children: (
                      <Space direction="vertical" size={10} style={{ width: '100%' }}>
                        <Paragraph className="compact-paragraph">
                          {item.runtime?.prompt_summary || '暂无提示词摘要。'}
                        </Paragraph>
                        <pre className="prompt-block">{String(item.runtime?.system_prompt || '暂无系统提示词配置')}</pre>
                        <pre className="prompt-block">{JSON.stringify(item.runtime?.output_contract || {}, null, 2)}</pre>
                      </Space>
                    ),
                  },
                ]}
              />
            </Space>
          </div>
        </Col>
      ))}
    </Row>
  </Card>
)

export default AgentConfigPanel
