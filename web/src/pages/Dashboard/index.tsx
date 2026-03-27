import React from 'react'
import { Row, Col, Card, Statistic, Table, Tag, Progress, Typography, Empty, Alert } from 'antd'
import {
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  CloseCircleOutlined,
  DashboardOutlined,
  BellOutlined,
  DatabaseOutlined,
} from '@ant-design/icons'
import { useQuery } from '@tanstack/react-query'
import type { TableProps } from 'antd'
import ReactECharts from 'echarts-for-react'
import { alertsApi, devicesApi, type AlertRecord, type DeviceRecord } from '../../lib/api'
import './style.css'

type DeviceStatus = 'online' | 'warning' | 'offline'

interface DashboardDevice {
  id: string
  name: string
  status: DeviceStatus
  tagCount: number
  uptime: number
}

const severityMeta: Record<AlertRecord['severity'], { color: string; label: string }> = {
  critical: { color: 'red', label: '严重' },
  warning: { color: 'orange', label: '警告' },
  info: { color: 'blue', label: '提示' },
}

const statusMeta: Record<DeviceStatus, { color: string; label: string }> = {
  online: { color: 'success', label: '在线' },
  warning: { color: 'warning', label: '关注' },
  offline: { color: 'error', label: '离线' },
}

const normalizeDeviceStatus = (status: DeviceRecord['status']): DeviceStatus => {
  if (status === 'error') {
    return 'warning'
  }
  return status
}

const buildUptime = (status: DeviceStatus) => {
  switch (status) {
    case 'online':
      return 98
    case 'warning':
      return 84
    case 'offline':
      return 61
  }
}

const DeviceCard: React.FC<{ device: DashboardDevice }> = ({ device }) => {
  const getStatusIcon = (status: DeviceStatus) => {
    switch (status) {
      case 'online':
        return <CheckCircleOutlined style={{ color: '#52c41a', fontSize: 24 }} />
      case 'warning':
        return <ExclamationCircleOutlined style={{ color: '#faad14', fontSize: 24 }} />
      case 'offline':
        return <CloseCircleOutlined style={{ color: '#f5222d', fontSize: 24 }} />
    }
  }

  return (
    <Card className="device-card" size="small">
      <div className="device-card-content">
        {getStatusIcon(device.status)}
        <div className="device-info">
          <div className="device-name">{device.name}</div>
          <div className="device-tags">
            {device.tagCount} 个测点 · <Tag color={statusMeta[device.status].color}>{statusMeta[device.status].label}</Tag>
          </div>
        </div>
      </div>
      <Progress
        percent={device.uptime}
        size="small"
        strokeColor={device.status === 'offline' ? '#f5222d' : '#1890ff'}
        style={{ marginTop: 12 }}
      />
    </Card>
  )
}

const TrendChart: React.FC = () => {
  const option = {
    title: { text: '24 小时关键指标趋势', textStyle: { fontSize: 14 } },
    tooltip: { trigger: 'axis' },
    grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: ['00:00', '04:00', '08:00', '12:00', '16:00', '20:00', '24:00'],
    },
    yAxis: { type: 'value' },
    series: [
      {
        name: '溶解氧',
        type: 'line',
        smooth: true,
        data: [3.2, 3.8, 4.1, 3.9, 4.2, 3.7, 3.5],
        itemStyle: { color: '#1890ff' },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 0,
            y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(24,144,255,0.3)' },
              { offset: 1, color: 'rgba(24,144,255,0.05)' },
            ],
          },
        },
      },
      {
        name: 'pH',
        type: 'line',
        smooth: true,
        data: [7.2, 7.1, 7.3, 7.2, 7.4, 7.1, 7.2],
        itemStyle: { color: '#52c41a' },
      },
    ],
  }

  return <ReactECharts option={option} style={{ height: 300 }} />
}

const AlertTable: React.FC<{ alerts: AlertRecord[]; loading: boolean }> = ({ alerts, loading }) => {
  const columns: TableProps<AlertRecord>['columns'] = [
    {
      title: '级别',
      dataIndex: 'severity',
      key: 'severity',
      width: 90,
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
      ellipsis: true,
    },
    {
      title: '时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 180,
      render: (value: string) => new Date(value).toLocaleString(),
    },
  ]

  return (
    <Table
      columns={columns}
      dataSource={alerts}
      rowKey="id"
      loading={loading}
      locale={{ emptyText: <Empty description="暂无告警数据" /> }}
      size="small"
      pagination={false}
      scroll={{ y: 200 }}
    />
  )
}

const Dashboard: React.FC = () => {
  const devicesQuery = useQuery({
    queryKey: ['devices'],
    queryFn: devicesApi.list,
  })

  const alertsQuery = useQuery({
    queryKey: ['alerts', 'recent'],
    queryFn: alertsApi.list,
  })

  const alertStatsQuery = useQuery({
    queryKey: ['alerts', 'stats'],
    queryFn: alertsApi.stats,
  })

  const deviceItems: DashboardDevice[] = (devicesQuery.data?.devices ?? []).map((device) => {
    const status = normalizeDeviceStatus(device.status)
    return {
      id: device.id,
      name: device.name,
      status,
      tagCount: device.tag_count,
      uptime: buildUptime(status),
    }
  })

  const onlineCount = deviceItems.filter((device) => device.status === 'online').length
  const warningCount = deviceItems.filter((device) => device.status === 'warning').length
  const offlineCount = deviceItems.filter((device) => device.status === 'offline').length
  const totalTags = deviceItems.reduce((sum, device) => sum + device.tagCount, 0)
  const recentAlerts = (alertsQuery.data?.alerts ?? []).slice(0, 5)

  return (
    <div className="dashboard-page">
      {devicesQuery.isError || alertsQuery.isError || alertStatsQuery.isError ? (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
          message="部分实时数据加载失败"
          description="后端接口已接通，但仍有部分资源返回异常。页面会优先展示当前拿到的数据。"
        />
      ) : null}

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} sm={12} lg={6}>
          <Card loading={devicesQuery.isLoading}>
            <Statistic
              title="设备总数"
              value={deviceItems.length}
              prefix={<DashboardOutlined />}
              suffix={<span style={{ fontSize: 14, color: '#52c41a' }}>({onlineCount} 在线)</span>}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card loading={devicesQuery.isLoading}>
            <Statistic title="监控点位" value={totalTags} prefix={<DatabaseOutlined />} />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card loading={alertStatsQuery.isLoading}>
            <Statistic
              title="活动告警"
              value={alertStatsQuery.data?.active_alerts ?? 0}
              prefix={<BellOutlined />}
              valueStyle={{ color: '#f5222d' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card loading={alertsQuery.isLoading}>
            <Statistic title="24小时数据量" value={1105920} suffix="条" />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} lg={8}>
          <Card title="设备状态" className="dashboard-card" loading={devicesQuery.isLoading}>
            <Typography.Paragraph type="secondary" style={{ marginBottom: 16 }}>
              当前共有 {onlineCount} 台在线，{warningCount} 台待关注，{offlineCount} 台离线。
            </Typography.Paragraph>
            {deviceItems.length > 0 ? (
              <div className="device-grid">
                {deviceItems.map((device) => (
                  <DeviceCard key={device.id} device={device} />
                ))}
              </div>
            ) : (
              <Empty description="暂无设备数据" />
            )}
          </Card>
        </Col>
        <Col xs={24} lg={16}>
          <Card title="实时趋势" className="dashboard-card">
            <TrendChart />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col xs={24}>
          <Card title="最近告警" className="dashboard-card">
            <AlertTable alerts={recentAlerts} loading={alertsQuery.isLoading} />
          </Card>
        </Col>
      </Row>
    </div>
  )
}

export default Dashboard
