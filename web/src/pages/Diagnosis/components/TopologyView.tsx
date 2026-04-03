import React from 'react'
import { Card, Typography } from 'antd'
import type { AgentCatalogItem, DiagnosisExpertOpinion, DiagnosisTaskTraceEvent } from '../../../lib/api'

const { Text } = Typography

interface Props {
  useGraphRag: boolean
  agents: AgentCatalogItem[]
  latestOpinions: DiagnosisExpertOpinion[]
  executionTrace: DiagnosisTaskTraceEvent[]
}

const nodeLabel = (status: string) => {
  const labels: Record<string, string> = {
    done: '已完成',
    running: '运行中',
    pending: '待执行',
    off: '已关闭',
  }
  return labels[status] || status
}

const TopologyView: React.FC<Props> = ({ useGraphRag, agents, latestOpinions, executionTrace }) => {
  const expertAgents = agents.filter((item) => item.type !== 'coordinator')
  const coordinator = agents.find((item) => item.type === 'coordinator')
  const graphNodeStatus = executionTrace.some((event) => event.stage === 'graph_rag_completed')
    ? 'done'
    : executionTrace.some((event) => event.stage === 'graph_rag_started')
      ? 'running'
      : useGraphRag
        ? 'pending'
        : 'off'
  const coordinatorNodeStatus = executionTrace.some((event) => event.stage === 'coordinator_completed' || event.stage === 'debate_completed')
    ? 'done'
    : executionTrace.some((event) => event.stage === 'coordinator_started' || event.stage === 'debate_started')
      ? 'running'
      : 'pending'
  const outputNodeStatus = executionTrace.some((event) => event.stage === 'diagnosis_completed' || event.stage === 'debate_completed') ? 'done' : 'pending'

  return (
    <Card title="协作拓扑" className="studio-card">
      <div className="topology-grid">
        <div className="topology-column">
          <div className={`topology-node topology-node--${graphNodeStatus}`}>
            <Text strong>GraphRAG</Text>
            <div>
              <Text type="secondary">{nodeLabel(graphNodeStatus)}</Text>
            </div>
          </div>
        </div>
        <div className="topology-column topology-column--experts">
          {expertAgents.map((item) => {
            const status = latestOpinions.some((opinion) => opinion.expert_type === item.type)
              ? 'done'
              : executionTrace.some(
                    (event) =>
                      (event.agent_key || event.agent_name) === item.type &&
                      (event.stage === 'expert_started' || event.stage === 'debate_agent_started'),
                  )
                ? 'running'
                : 'pending'
            return (
              <div key={item.id} className={`topology-node topology-node--${status}`}>
                <Text strong>{item.name}</Text>
                <div>
                  <Text type="secondary">{item.runtime?.model_name || '未绑定'}</Text>
                </div>
                <div>
                  <Text type="secondary">{nodeLabel(status)}</Text>
                </div>
              </div>
            )
          })}
        </div>
        <div className="topology-column">
          <div className={`topology-node topology-node--${coordinatorNodeStatus}`}>
            <Text strong>{coordinator?.name || '协调者'}</Text>
            <div>
              <Text type="secondary">{coordinator?.runtime?.model_name || '未绑定'}</Text>
            </div>
            <div>
              <Text type="secondary">{nodeLabel(coordinatorNodeStatus)}</Text>
            </div>
          </div>
        </div>
        <div className="topology-column">
          <div className={`topology-node topology-node--${outputNodeStatus}`}>
            <Text strong>Final Output</Text>
            <div>
              <Text type="secondary">{nodeLabel(outputNodeStatus)}</Text>
            </div>
          </div>
        </div>
      </div>
    </Card>
  )
}

export default TopologyView
