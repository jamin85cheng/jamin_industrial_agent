import React from 'react'
import { Card, Table, Tag, Button, Badge, Space, message, Statistic, Row, Col, Empty } from 'antd'
import { CheckCircleOutlined, BellOutlined } from '@ant-design/icons'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { TableProps } from 'antd'
import { alertsApi, extractApiError, type AlertRecord } from '../../lib/api'

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

  const columns: TableProps<AlertRecord>['columns'] = [
    {
      title: '告警 ID',
      dataIndex: 'id',
      key: 'id',
    },
    {
      title: '级别',
      dataIndex: 'severity',
      key: 'severity',
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
      render: (value: string | null | undefined) => value || '-',
    },
    {
      title: '时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (value: string) => new Date(value).toLocaleString(),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: AlertRecord['status']) => {
        const meta = statusMeta[status]
        return status === 'active' ? (
          <Badge status={meta.color as 'processing'} text={meta.label} />
        ) : (
          <Tag color={meta.color}>{meta.label}</Tag>
        )
      },
    },
    {
      title: '操作',
      key: 'action',
      render: (_, record) => (
        <Space>
          {record.status === 'active' && (
            <Button
              type="primary"
              size="small"
              icon={<CheckCircleOutlined />}
              loading={acknowledgeMutation.isPending}
              onClick={() => acknowledgeMutation.mutate(record.id)}
            >
              确认
            </Button>
          )}
          <Button size="small" onClick={() => message.info(record.message)}>
            详情
          </Button>
        </Space>
      ),
    },
  ]

  const alerts = alertsQuery.data?.alerts ?? []

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <Row gutter={[16, 16]}>
        <Col xs={24} md={8}>
          <Card loading={statsQuery.isLoading}>
            <Statistic title="总告警数" value={statsQuery.data?.total_alerts ?? 0} prefix={<BellOutlined />} />
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
              title="已确认/已解决"
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
          <Space>
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
            <Button type="primary" onClick={() => message.success('导出功能将在接入报表服务后启用')}>
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
          pagination={{ pageSize: 6 }}
          locale={{ emptyText: <Empty description="暂无告警数据" /> }}
        />
      </Card>
    </div>
  )
}

export default Alerts
