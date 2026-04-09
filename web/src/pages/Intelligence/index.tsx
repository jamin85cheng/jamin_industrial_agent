import React from 'react'
import {
  Alert,
  Button,
  Card,
  Col,
  Empty,
  List,
  Progress,
  Row,
  Space,
  Statistic,
  Table,
  Tabs,
  Tag,
  Typography,
  message,
} from 'antd'
import {
  CheckCircleOutlined,
  ClusterOutlined,
  DatabaseOutlined,
  DeploymentUnitOutlined,
  ExperimentOutlined,
  PlayCircleOutlined,
  RadarChartOutlined,
  ReloadOutlined,
  SafetyCertificateOutlined,
} from '@ant-design/icons'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { TableProps } from 'antd'
import {
  extractApiError,
  intelligenceApi,
  type IntelligenceAssetAssessment,
  type IntelligenceKnowledgeCase,
  type IntelligenceLearningCandidate,
  type IntelligencePatrolRun,
  type IntelligenceSnapshot,
} from '../../lib/api'
import { useAuthStore } from '../../stores/auth'
import './style.css'

const { Paragraph, Text, Title } = Typography

const riskMeta: Record<string, { color: string; label: string }> = {
  normal: { color: 'success', label: '正常' },
  attention: { color: 'gold', label: '关注' },
  warning: { color: 'orange', label: '预警' },
  high_risk: { color: 'red', label: '高风险' },
  needs_review: { color: 'magenta', label: '待复核' },
}

const candidateMeta: Record<string, { color: string; label: string }> = {
  draft: { color: 'default', label: '草稿' },
  ready_for_shadow: { color: 'processing', label: '可影子发布' },
  awaiting_review: { color: 'gold', label: '待评审' },
  collecting_samples: { color: 'cyan', label: '样本积累中' },
}

const fallbackRiskMeta = { color: 'default', label: '未知' }

const formatDateTime = (value?: string | null) => {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString()
}

const canReviewLabels = (permissions?: string[], roles?: string[]) =>
  Boolean(
    permissions?.includes('*') ||
      permissions?.includes('alert:acknowledge') ||
      roles?.includes('admin'),
  )

const canGenerateCandidates = (permissions?: string[], roles?: string[]) =>
  Boolean(
    permissions?.includes('*') ||
      permissions?.includes('knowledge:write') ||
      roles?.includes('admin') ||
      roles?.includes('engineer'),
  )

const getKnowledgeCaseSubtitle = (item: IntelligenceKnowledgeCase) => [
  item.scene_type,
  item.root_cause || '',
  item.source_type,
  item.usage_count ? `引用 ${item.usage_count}` : '',
]
  .filter(Boolean)
  .join(' · ')

const getCandidateSubtitle = (item: IntelligenceLearningCandidate) => {
  const projects = Array.isArray(item.payload?.source_projects)
    ? (item.payload.source_projects as string[])
    : []
  return [item.candidate_type, ...projects.slice(0, 3)].filter(Boolean).join(' · ')
}

const summarizeSnapshot = (snapshot: IntelligenceSnapshot) =>
  Object.values(snapshot.points || {})
    .slice(0, 4)
    .map((point) => `${point.display_name}: ${point.value}${point.unit || ''}`)
    .join(' · ')

const PatrolRunList: React.FC<{ runs: IntelligencePatrolRun[]; loading?: boolean }> = ({ runs, loading }) => (
  <List
    loading={loading}
    locale={{ emptyText: <Empty description="暂无巡检批次" /> }}
    dataSource={runs}
    renderItem={(item) => {
      const meta = riskMeta[item.risk_level] || fallbackRiskMeta
      return (
        <List.Item>
          <div className="intelligence-run-item">
            <div className="intelligence-run-main">
              <Space wrap>
                <Text strong>{item.run_id}</Text>
                <Tag color={meta.color}>{meta.label}</Tag>
                <Tag>{item.schedule_type}</Tag>
              </Space>
              <Text type="secondary">
                {formatDateTime(item.created_at)} · 异常资产 {item.abnormal_asset_count} · 待审核 {item.review_queue_size}
              </Text>
            </div>
            <div className="intelligence-run-score">
              <span>风险评分</span>
              <strong>{item.risk_score}</strong>
            </div>
          </div>
        </List.Item>
      )
    }}
  />
)

const Intelligence: React.FC = () => {
  const queryClient = useQueryClient()
  const user = useAuthStore((state) => state.user)
  const permissions = user?.permissions ?? []
  const roles = user?.roles ?? []
  const allowReview = canReviewLabels(permissions, roles)
  const allowCandidateGeneration = canGenerateCandidates(permissions, roles)

  const runtimeQuery = useQuery({
    queryKey: ['intelligence', 'runtime'],
    queryFn: intelligenceApi.getRuntime,
    refetchInterval: 15_000,
  })
  const assetsQuery = useQuery({
    queryKey: ['intelligence', 'assets'],
    queryFn: intelligenceApi.listAssets,
    staleTime: 60_000,
  })
  const latestRiskQuery = useQuery({
    queryKey: ['intelligence', 'latest-risk'],
    queryFn: intelligenceApi.getLatestRisk,
    refetchInterval: 15_000,
  })
  const reviewQueueQuery = useQuery({
    queryKey: ['intelligence', 'review-queue', 'pending'],
    queryFn: () => intelligenceApi.listReviewQueue({ status: 'pending', limit: 20 }),
    refetchInterval: 15_000,
  })
  const knowledgeCasesQuery = useQuery({
    queryKey: ['intelligence', 'knowledge-cases'],
    queryFn: () => intelligenceApi.listKnowledgeCases({ limit: 20 }),
    refetchInterval: 30_000,
  })
  const candidatesQuery = useQuery({
    queryKey: ['intelligence', 'learning-candidates'],
    queryFn: () => intelligenceApi.listLearningCandidates({ limit: 20 }),
    refetchInterval: 30_000,
  })
  const snapshotsQuery = useQuery({
    queryKey: ['intelligence', 'snapshots'],
    queryFn: () => intelligenceApi.listLatestSnapshots(),
    refetchInterval: 10_000,
  })
  const patrolRunsQuery = useQuery({
    queryKey: ['intelligence', 'patrol-runs'],
    queryFn: () => intelligenceApi.listPatrolRuns(8),
    refetchInterval: 15_000,
  })

  const refreshAll = () => {
    void queryClient.invalidateQueries({ queryKey: ['intelligence'] })
  }

  const seedMutation = useMutation({
    mutationFn: intelligenceApi.seedDemo,
    onSuccess: (payload) => {
      message.success(
        payload.patrol_run
          ? `已注入 ${payload.profile} 样本并完成巡检`
          : `已注入 ${payload.profile} 样本`,
      )
      refreshAll()
    },
    onError: (error) => {
      message.error(extractApiError(error, '演示样本注入失败'))
    },
  })

  const patrolMutation = useMutation({
    mutationFn: intelligenceApi.runPatrol,
    onSuccess: (payload) => {
      message.success(`巡检完成，批次 ${payload.run_id}`)
      refreshAll()
    },
    onError: (error) => {
      message.error(extractApiError(error, '巡检执行失败'))
    },
  })

  const reviewMutation = useMutation({
    mutationFn: ({ labelId, payload }: { labelId: string; payload: Parameters<typeof intelligenceApi.reviewLabel>[1] }) =>
      intelligenceApi.reviewLabel(labelId, payload),
    onSuccess: (payload) => {
      message.success(
        payload.knowledge_case
          ? '标签已确认，并已沉淀为知识案例'
          : '标签审核结果已更新',
      )
      refreshAll()
    },
    onError: (error) => {
      message.error(extractApiError(error, '审核标签失败'))
    },
  })

  const candidateMutation = useMutation({
    mutationFn: intelligenceApi.generateLearningCandidates,
    onSuccess: (payload) => {
      message.success(`已生成 ${payload.length} 个学习候选`)
      refreshAll()
    },
    onError: (error) => {
      message.error(extractApiError(error, '生成学习候选失败'))
    },
  })

  const assetResults = latestRiskQuery.data?.asset_results ?? []
  const reviewQueue = reviewQueueQuery.data ?? []
  const patrolRuns = patrolRunsQuery.data ?? []
  const snapshots = snapshotsQuery.data ?? []
  const knowledgeCases = knowledgeCasesQuery.data ?? []
  const candidates = candidatesQuery.data ?? []
  const runtimeSummary = runtimeQuery.data?.service
  const schedulerSummary = runtimeQuery.data?.scheduler

  const assessmentColumns: TableProps<IntelligenceAssetAssessment>['columns'] = [
    {
      title: '资产',
      dataIndex: 'asset_name',
      key: 'asset_name',
      width: 180,
      render: (value: string, record) => (
        <div>
          <div>{value}</div>
          <Text type="secondary">{record.scene_type}</Text>
        </div>
      ),
    },
    {
      title: '状态',
      dataIndex: 'risk_level',
      key: 'risk_level',
      width: 120,
      render: (value: string, record) => {
        const meta = riskMeta[record.status] || riskMeta[value] || fallbackRiskMeta
        return <Tag color={meta.color}>{meta.label}</Tag>
      },
    },
    {
      title: '风险',
      dataIndex: 'risk_score',
      key: 'risk_score',
      width: 180,
      render: (value: number) => <Progress percent={Math.round(value)} size="small" status={value >= 70 ? 'exception' : value >= 40 ? 'active' : 'normal'} />,
    },
    {
      title: '疑似问题',
      dataIndex: 'suspected_faults',
      key: 'suspected_faults',
      render: (value: string[]) =>
        value?.length ? (
          <Space wrap>
            {value.slice(0, 3).map((item) => (
              <Tag key={item}>{item}</Tag>
            ))}
          </Space>
        ) : (
          <Text type="secondary">无异常</Text>
        ),
    },
    {
      title: '外部锚定',
      dataIndex: 'knowledge_grounding_ratio',
      key: 'knowledge_grounding_ratio',
      width: 140,
      render: (value: number) => `${Math.round((value || 0) * 100)}%`,
    },
  ]

  return (
    <div className="intelligence-page">
      <section className="intelligence-hero">
        <div className="intelligence-hero-copy">
          <Tag color="blue">Read-Only Patrol</Tag>
          <Title level={2}>工业智能巡检台</Title>
          <Paragraph>
            面向粉尘处理一期场景的智能工作台。每 3 分钟可自动巡检一次最新 PLC 快照，输出风险评分、
            问题点位、人工审核队列、知识案例和学习候选。
          </Paragraph>
          <Space wrap>
            <Button
              icon={<DatabaseOutlined />}
              loading={seedMutation.isPending}
              onClick={() => seedMutation.mutate({ profile: 'normal', run_patrol: false })}
            >
              注入正常样本
            </Button>
            <Button
              type="primary"
              icon={<PlayCircleOutlined />}
              loading={seedMutation.isPending}
              onClick={() => seedMutation.mutate({ profile: 'warning', run_patrol: true })}
            >
              注入关注样本并巡检
            </Button>
            <Button
              danger
              icon={<SafetyCertificateOutlined />}
              loading={seedMutation.isPending}
              onClick={() => seedMutation.mutate({ profile: 'critical', run_patrol: true })}
            >
              注入高风险样本并巡检
            </Button>
            <Button
              icon={<RadarChartOutlined />}
              loading={patrolMutation.isPending}
              onClick={() => patrolMutation.mutate({ schedule_type: 'manual' })}
            >
              立即巡检
            </Button>
            <Button
              icon={<ExperimentOutlined />}
              disabled={!allowCandidateGeneration}
              loading={candidateMutation.isPending}
              onClick={() => candidateMutation.mutate(['workflow', 'prompt', 'model'])}
            >
              生成学习候选
            </Button>
            <Button icon={<ReloadOutlined />} onClick={refreshAll}>
              刷新视图
            </Button>
          </Space>
          {!allowCandidateGeneration ? (
            <Paragraph type="secondary" style={{ marginTop: 12, marginBottom: 0 }}>
              当前账号缺少 `knowledge:write` 权限，仍可查看巡检与审核结果，但不能生成学习候选。
            </Paragraph>
          ) : null}
        </div>

        <div className="intelligence-hero-board">
          <div className="intelligence-signal">
            <span>调度状态</span>
            <strong>{schedulerSummary?.running ? '运行中' : '未启动'}</strong>
          </div>
          <div className="intelligence-signal">
            <span>巡检间隔</span>
            <strong>{runtimeSummary?.patrol_interval_seconds ?? '-'}s</strong>
          </div>
          <div className="intelligence-signal">
            <span>待审核标签</span>
            <strong>{runtimeSummary?.pending_review_count ?? 0}</strong>
          </div>
          <div className="intelligence-signal">
            <span>知识案例</span>
            <strong>{runtimeSummary?.knowledge_case_count ?? 0}</strong>
          </div>
        </div>
      </section>

      {schedulerSummary?.last_error ? (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
          message="调度器最近一次执行出现异常"
          description={schedulerSummary.last_error}
        />
      ) : null}

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} sm={12} xl={6}>
          <Card className="intelligence-card">
            <Statistic title="资产数" value={runtimeSummary?.assets ?? assetsQuery.data?.assets.length ?? 0} prefix={<DeploymentUnitOutlined />} />
          </Card>
        </Col>
        <Col xs={24} sm={12} xl={6}>
          <Card className="intelligence-card">
            <Statistic title="已确认标签" value={runtimeSummary?.confirmed_label_count ?? 0} prefix={<CheckCircleOutlined />} />
          </Card>
        </Col>
        <Col xs={24} sm={12} xl={6}>
          <Card className="intelligence-card">
            <Statistic title="学习候选" value={runtimeSummary?.candidate_count ?? 0} prefix={<ClusterOutlined />} />
          </Card>
        </Col>
        <Col xs={24} sm={12} xl={6}>
          <Card className="intelligence-card">
            <Statistic title="最近巡检" value={latestRiskQuery.data?.run_id ? formatDateTime(patrolRuns[0]?.created_at) : '暂无'} prefix={<RadarChartOutlined />} />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col xs={24} xl={15}>
          <Card
            className="intelligence-card"
            title="最新风险评估"
            extra={<Text type="secondary">最新批次 {latestRiskQuery.data?.run_id || '-'}</Text>}
          >
            <Table
              rowKey="asset_id"
              columns={assessmentColumns}
              dataSource={assetResults}
              pagination={false}
              locale={{ emptyText: <Empty description="暂无巡检结果，先注入样本或手动巡检" /> }}
              expandable={{
                expandedRowRender: (record) => (
                  <div className="intelligence-expand">
                    <Paragraph style={{ marginBottom: 8 }}>{record.summary}</Paragraph>
                    <Space wrap style={{ marginBottom: 8 }}>
                      {record.operator_actions.map((item) => (
                        <Tag key={item} color="blue">
                          {item}
                        </Tag>
                      ))}
                    </Space>
                    <Text type="secondary">
                      影响点位: {record.affected_points.length ? record.affected_points.join(', ') : '无'}
                    </Text>
                  </div>
                ),
              }}
            />
          </Card>
        </Col>

        <Col xs={24} xl={9}>
          <Card
            className="intelligence-card"
            title="待审核队列"
            extra={<Tag color={allowReview ? 'processing' : 'default'}>{allowReview ? '可审核' : '只读'}</Tag>}
          >
            <List
              locale={{ emptyText: <Empty description="当前没有待审核标签" /> }}
              dataSource={reviewQueue}
              renderItem={(item) => (
                <List.Item
                  actions={[
                    <Button
                      key="confirm"
                      type="link"
                      disabled={!allowReview}
                      loading={reviewMutation.isPending}
                      onClick={() =>
                        reviewMutation.mutate({
                          labelId: item.label_id,
                          payload: {
                            anomaly_type:
                              item.current_assessment?.suspected_faults?.[0] || item.anomaly_type || undefined,
                            root_cause:
                              item.current_assessment?.suspected_faults?.[0] || item.root_cause || undefined,
                            review_notes: '通过巡检台快速确认',
                            final_action: item.current_assessment?.operator_actions?.[0] || '',
                          },
                        })
                      }
                    >
                      确认
                    </Button>,
                    <Button
                      key="reject"
                      type="link"
                      danger
                      disabled={!allowReview}
                      loading={reviewMutation.isPending}
                      onClick={() =>
                        reviewMutation.mutate({
                          labelId: item.label_id,
                          payload: {
                            false_positive: true,
                            review_notes: '通过巡检台标记为误报',
                          },
                        })
                      }
                    >
                      误报
                    </Button>,
                  ]}
                >
                  <div className="intelligence-review-item">
                    <Space wrap>
                      <Text strong>{item.asset_name || item.asset_id}</Text>
                      <Tag color={(riskMeta[item.current_assessment?.risk_level || item.status] || fallbackRiskMeta).color}>
                        {(riskMeta[item.current_assessment?.risk_level || item.status] || fallbackRiskMeta).label}
                      </Tag>
                    </Space>
                    <Text type="secondary">
                      {item.anomaly_type || item.current_assessment?.suspected_faults?.[0] || '待确认问题'}
                    </Text>
                    <Paragraph ellipsis={{ rows: 2, expandable: false }} style={{ marginBottom: 8 }}>
                      {item.current_assessment?.summary || '等待人工审核确认根因与最终动作。'}
                    </Paragraph>
                    <Text type="secondary">更新时间 {formatDateTime(item.updated_at as string)}</Text>
                  </div>
                </List.Item>
              )}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
        <Col xs={24} xl={16}>
          <Card className="intelligence-card" title="知识、候选与快照">
            <Tabs
              items={[
                {
                  key: 'knowledge',
                  label: '知识案例',
                  children: (
                    <List
                      locale={{ emptyText: <Empty description="暂无知识案例" /> }}
                      dataSource={knowledgeCases}
                      renderItem={(item: IntelligenceKnowledgeCase) => (
                        <List.Item>
                          <div className="intelligence-list-block">
                            <Space wrap>
                              <Text strong>{item.title}</Text>
                              <Tag>{item.scene_type}</Tag>
                              <Tag color="blue">{item.source_type}</Tag>
                            </Space>
                            <Text type="secondary">{getKnowledgeCaseSubtitle(item)}</Text>
                            <Paragraph style={{ marginBottom: 0 }}>{item.summary}</Paragraph>
                          </div>
                        </List.Item>
                      )}
                    />
                  ),
                },
                {
                  key: 'candidates',
                  label: '学习候选',
                  children: (
                    <List
                      locale={{ emptyText: <Empty description="暂无学习候选" /> }}
                      dataSource={candidates}
                      renderItem={(item: IntelligenceLearningCandidate) => {
                        const meta = candidateMeta[item.status] || { color: 'default', label: item.status }
                        return (
                          <List.Item>
                            <div className="intelligence-list-block">
                              <Space wrap>
                                <Text strong>{item.name}</Text>
                                <Tag>{item.candidate_type}</Tag>
                                <Tag color={meta.color}>{meta.label}</Tag>
                                <Tag color="geekblue">Score {item.score}</Tag>
                              </Space>
                              <Text type="secondary">{getCandidateSubtitle(item)}</Text>
                              <Paragraph style={{ marginBottom: 0 }}>{item.rationale}</Paragraph>
                            </div>
                          </List.Item>
                        )
                      }}
                    />
                  ),
                },
                {
                  key: 'snapshots',
                  label: '最新快照',
                  children: (
                    <List
                      locale={{ emptyText: <Empty description="暂无最新快照" /> }}
                      dataSource={snapshots}
                      renderItem={(item: IntelligenceSnapshot) => (
                        <List.Item>
                          <div className="intelligence-list-block">
                            <Space wrap>
                              <Text strong>{item.asset_name || item.asset_id}</Text>
                              <Tag>{item.source}</Tag>
                              <Tag color="cyan">完整度 {Math.round((item.completeness || 0) * 100)}%</Tag>
                            </Space>
                            <Text type="secondary">{formatDateTime(item.collected_at)}</Text>
                            <Paragraph style={{ marginBottom: 0 }}>{summarizeSnapshot(item)}</Paragraph>
                          </div>
                        </List.Item>
                      )}
                    />
                  ),
                },
              ]}
            />
          </Card>
        </Col>

        <Col xs={24} xl={8}>
          <Card className="intelligence-card" title="最近巡检批次">
            <PatrolRunList runs={patrolRuns} loading={patrolRunsQuery.isLoading} />
          </Card>
        </Col>
      </Row>
    </div>
  )
}

export default Intelligence
