import React from 'react'
import { useNavigate } from 'react-router-dom'
import { Badge, Button, Card, Col, Empty, Row, Space, Statistic, Table, Tag, message } from 'antd'
import { BellOutlined, CheckCircleOutlined, ThunderboltOutlined } from '@ant-design/icons'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { TableProps } from 'antd'
import { alertsApi, diagnosisV2Api, extractApiError, type AlertRecord } from '../../lib/api'

const severityMeta: Record<AlertRecord['severity'], { color: string; label: string }> = {
  critical: { color: 'red', label: '严重' },
  warning: { color: 'orange', label: '警告' },
  info: { color: 'blue', label: '提示' },
}

const statusMeta: Record<AlertRecord['status'], { color: string; label: string }> = {
  active: { color: 'processing', label: '待确认' },
  acknowledged: { color: 'success', label: '已确认' },
  resolved: { color: 'default', label: '已解决' },
}

const Alerts: React.FC = () => {
  const navigate = useNavigate()
  const queryClient = useQueryClient()

  const alertsQuery = useQuery({
    queryKey: ['alerts'],
    queryFn: alertsApi.list,
  })

  const statsQuery = useQuery({
    queryKey: ['alerts', 'stats'],
    queryFn: alertsApi.stats,
  })

  const acknowledgeMutation = useMutation({
    mutationFn: (alertId: string) => alertsApi.acknowledge(alertId),
    onSuccess: () => {
      message.success('告警已确认')
      void queryClient.invalidateQueries({ queryKey: ['alerts'] })
      void queryClient.invalidateQueries({ queryKey: ['alerts', 'stats'] })
    },
    onError: (error) => {
      message.error(extractApiError(error, '确认告警失败'))
    },
  })

  const diagnoseMutation = useMutation({
    mutationFn: (alertId: string) =>
      diagnosisV2Api.analyzeAlert(alertId, {
        use_graph_rag: true,
        use_camel: false,
        debug: true,
        priority: 'high',
      }),
    onSuccess: (response, alertId) => {
      if (!response.task_id) {
        message.warning('诊断任务已创建，但未返回任务编号')
        return
      }
      message.success(`已从告警 ${alertId} 发起多智能体诊断`)
      navigate(`/diagnosis?taskId=${encodeURIComponent(response.task_id)}`)
    },
    onError: (error) => {
      message.error(extractApiError(error, '从告警发起诊断失败'))
    },
  })

  const alerts = alertsQuery.data?.alerts ?? []

  const columns: TableProps<AlertRecord>['columns'] = [
    {
      title: '告警 ID',
      dataIndex: 'id',
      key: 'id',
      width: 190,
    },
    {
      title: '级别',
      dataIndex: 'severity',
      key: 'severity',
      width: 100,
      render: (severity: AlertRecord['severity']) => {
        const meta = severityMeta[severity]
        return <Tag color={meta.color}>{meta.label}</Tag>
      },
    },
    {
      title: '规则名称',
      dataIndex: 'rule_name',
      key: 'rule_name',
      render: (value: string | null | undefined) => value || '-',
    },
    {
      title: '描述',
      dataIndex: 'message',
      key: 'message',
    },
    {
      title: '设备',
      dataIndex: 'device_id',
      key: 'device_id',
      width: 160,
      render: (value: string | null | undefined) => value || '-',
    },
    {
      title: '时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 180,
      render: (value: string) => new Date(value).toLocaleString(),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: AlertRecord['status']) => {
        const meta = statusMeta[status]
        return status === 'active' ? <Badge status={meta.color as 'processing'} text={meta.label} /> : <Tag color={meta.color}>{meta.label}</Tag>
      },
    },
    {
      title: '操作',
      key: 'action',
      width: 240,
      render: (_, record) => (
        <Space wrap>
          <Button
            size="small"
            icon={<ThunderboltOutlined />}
            loading={diagnoseMutation.isPending}
            onClick={() => diagnoseMutation.mutate(record.id)}
          >
            发起诊断
          </Button>
          {record.status === 'active' ? (
            <Button
              type="primary"
              size="small"
              icon={<CheckCircleOutlined />}
              loading={acknowledgeMutation.isPending}
              onClick={() => acknowledgeMutation.mutate(record.id)}
            >
              确认
            </Button>
          ) : null}
          <Button size="small" onClick={() => message.info(record.message)}>
            详情
          </Button>
        </Space>
      ),
    },
  ]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <Row gutter={[16, 16]}>
        <Col xs={24} md={8}>
          <Card loading={statsQuery.isLoading}>
            <Statistic title="告警总数" value={statsQuery.data?.total_alerts ?? 0} prefix={<BellOutlined />} />
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card loading={statsQuery.isLoading}>
            <Statistic title="待确认" value={statsQuery.data?.active_alerts ?? 0} valueStyle={{ color: '#fa8c16' }} />
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card loading={statsQuery.isLoading}>
            <Statistic
              title="已确认 / 已解决"
              value={(statsQuery.data?.total_alerts ?? 0) - (statsQuery.data?.active_alerts ?? 0)}
              valueStyle={{ color: '#52c41a' }}
            />
          </Card>
        </Col>
      </Row>

      <Card
        title={
          <span>
            <BellOutlined /> 告警列表
          </span>
        }
        extra={
          <Space wrap>
            <Button
              onClick={() => {
                const firstActiveAlert = alerts.find((item) => item.status === 'active')
                if (!firstActiveAlert) {
                  message.info('当前没有待确认告警')
                  return
                }
                acknowledgeMutation.mutate(firstActiveAlert.id)
              }}
              disabled={!alerts.some((item) => item.status === 'active')}
            >
              快速确认一条
            </Button>
            <Button
              onClick={() => {
                const latestAlert = alerts[0]
                if (!latestAlert) {
                  message.info('当前没有可诊断的告警')
                  return
                }
                diagnoseMutation.mutate(latestAlert.id)
              }}
              disabled={!alerts.length}
              loading={diagnoseMutation.isPending}
            >
              从最新告警发起诊断
            </Button>
            <Button type="primary" onClick={() => message.success('导出能力将与报告服务统一整合后开放')}>
              导出报表
            </Button>
          </Space>
        }
      >
        <Table
          columns={columns}
          dataSource={alerts}
          rowKey="id"
          loading={alertsQuery.isLoading}
          locale={{ emptyText: <Empty description="暂无告警数据" /> }}
        />
      </Card>
    </div>
  )
}

export default Alerts
