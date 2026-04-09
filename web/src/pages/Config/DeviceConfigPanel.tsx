import React, { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Card,
  Col,
  Empty,
  Form,
  Input,
  InputNumber,
  Modal,
  Popconfirm,
  Row,
  Select,
  Space,
  Table,
  Tag,
  Typography,
  message,
} from 'antd'
import type { TableProps } from 'antd'
import {
  ApiOutlined,
  CheckCircleOutlined,
  CloudServerOutlined,
  DisconnectOutlined,
  EditOutlined,
  PlusOutlined,
  ReloadOutlined,
} from '@ant-design/icons'
import {
  devicesApi,
  extractApiError,
  type DeviceCreatePayload,
  type DeviceRecord,
  type DeviceTagImportPreviewResponse,
  type DeviceTagRecord,
  type DeviceUpdatePayload,
} from '../../lib/api'
import TagImportButton from './TagImportButton'

interface DeviceEditorValues {
  name: string
  type: 's7' | 'modbus' | 'simulated'
  host: string
  port: number
  rack?: number
  slot?: number
  scan_interval: number
  tags: DeviceTagRecord[]
}

const deviceTypeOptions = [
  { label: 'Siemens S7', value: 's7' },
  { label: 'Modbus TCP', value: 'modbus' },
  { label: 'Simulated', value: 'simulated' },
]

const deviceStatusMeta: Record<string, { color: string; label: string }> = {
  online: { color: 'green', label: '在线' },
  offline: { color: 'default', label: '离线' },
  error: { color: 'red', label: '异常' },
}

const createEmptyTag = (): DeviceTagRecord => ({
  name: '',
  address: '',
  data_type: 'float',
  unit: '',
  description: '',
  asset_id: '',
  point_key: '',
  deadband: null,
  debounce_ms: 0,
})

const createInitialDeviceValues = (): DeviceEditorValues => ({
  name: '',
  type: 's7',
  host: '127.0.0.1',
  port: 102,
  rack: 0,
  slot: 1,
  scan_interval: 10,
  tags: [createEmptyTag()],
})

const normalizeTags = (tags: DeviceTagRecord[]): DeviceTagRecord[] =>
  (tags ?? [])
    .map((tag) => ({
      name: tag.name?.trim(),
      address: tag.address?.trim(),
      data_type: tag.data_type || 'float',
      unit: tag.unit?.trim() || undefined,
      description: tag.description?.trim() || undefined,
      asset_id: tag.asset_id?.trim() || undefined,
      point_key: tag.point_key?.trim() || undefined,
      deadband: tag.deadband ?? undefined,
      debounce_ms: Number(tag.debounce_ms ?? 0),
    }))
    .filter((tag) => tag.name && tag.address)

const getImportedTags = (preview: DeviceTagImportPreviewResponse): DeviceTagRecord[] =>
  preview.tags.length > 0 ? preview.tags : [createEmptyTag()]

const DeviceTagListFields: React.FC = () => (
  <Form.List name="tags">
    {(fields, { add, remove }) => (
      <Space direction="vertical" size={12} style={{ width: '100%' }}>
        <Space style={{ justifyContent: 'space-between', width: '100%' }} wrap>
          <Typography.Text strong>点位与语义映射</Typography.Text>
          <Button type="dashed" icon={<PlusOutlined />} onClick={() => add(createEmptyTag())}>
            添加点位
          </Button>
        </Space>

        {fields.length === 0 ? (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="还没有点位，先添加一个映射" />
        ) : null}

        {fields.map((field, index) => (
          <Card
            key={field.key}
            size="small"
            title={`点位 ${index + 1}`}
            extra={
              <Button danger type="text" onClick={() => remove(field.name)}>
                删除
              </Button>
            }
            styles={{ body: { paddingBottom: 8 } }}
          >
            <Row gutter={12}>
              <Col xs={24} md={8}>
                <Form.Item
                  label="点位名"
                  name={[field.name, 'name']}
                  rules={[{ required: true, message: '请输入点位名' }]}
                >
                  <Input placeholder="fan_current" />
                </Form.Item>
              </Col>
              <Col xs={24} md={8}>
                <Form.Item
                  label="PLC 地址"
                  name={[field.name, 'address']}
                  rules={[{ required: true, message: '请输入 PLC 地址' }]}
                >
                  <Input placeholder="DB1.DBD0 / 40001 / SIM:1" />
                </Form.Item>
              </Col>
              <Col xs={24} md={8}>
                <Form.Item label="数据类型" name={[field.name, 'data_type']}>
                  <Select
                    options={[
                      { label: 'FLOAT', value: 'float' },
                      { label: 'INT', value: 'int' },
                      { label: 'BOOL', value: 'bool' },
                      { label: 'STRING', value: 'string' },
                    ]}
                  />
                </Form.Item>
              </Col>

              <Col xs={24} md={6}>
                <Form.Item label="单位" name={[field.name, 'unit']}>
                  <Input placeholder="A / kPa / mg/m3" />
                </Form.Item>
              </Col>
              <Col xs={24} md={6}>
                <Form.Item label="资产 ID" name={[field.name, 'asset_id']}>
                  <Input placeholder="ASSET_DUST_COLLECTOR_01" />
                </Form.Item>
              </Col>
              <Col xs={24} md={6}>
                <Form.Item label="point_key" name={[field.name, 'point_key']}>
                  <Input placeholder="dust_concentration_mg_m3" />
                </Form.Item>
              </Col>
              <Col xs={12} md={3}>
                <Form.Item label="Deadband" name={[field.name, 'deadband']}>
                  <InputNumber min={0} step={0.01} style={{ width: '100%' }} placeholder="0.05" />
                </Form.Item>
              </Col>
              <Col xs={12} md={3}>
                <Form.Item label="Debounce(ms)" name={[field.name, 'debounce_ms']}>
                  <InputNumber min={0} step={100} style={{ width: '100%' }} />
                </Form.Item>
              </Col>

              <Col xs={24}>
                <Form.Item label="说明 / 兼容元数据" name={[field.name, 'description']}>
                  <Input.TextArea
                    rows={2}
                    placeholder="可继续保留 value=...;history_policy=on_change 这类兼容元数据"
                  />
                </Form.Item>
              </Col>
            </Row>
          </Card>
        ))}
      </Space>
    )}
  </Form.List>
)

const DeviceConfigPanel: React.FC = () => {
  const [createForm] = Form.useForm<DeviceEditorValues>()
  const [editForm] = Form.useForm<DeviceEditorValues>()
  const [editingDevice, setEditingDevice] = useState<DeviceRecord | null>(null)
  const queryClient = useQueryClient()

  const devicesQuery = useQuery({
    queryKey: ['devices', 'config-panel'],
    queryFn: devicesApi.list,
  })

  const editingTagsQuery = useQuery({
    queryKey: ['devices', 'tags', editingDevice?.id],
    queryFn: () => devicesApi.listTags(editingDevice!.id),
    enabled: Boolean(editingDevice?.id),
  })

  useEffect(() => {
    createForm.setFieldsValue(createInitialDeviceValues())
  }, [createForm])

  useEffect(() => {
    if (!editingDevice) {
      return
    }

    editForm.setFieldsValue({
      name: editingDevice.name,
      type: editingDevice.type as DeviceEditorValues['type'],
      host: editingDevice.host,
      port: editingDevice.port,
      rack: editingDevice.rack ?? 0,
      slot: editingDevice.slot ?? 1,
      scan_interval: editingDevice.scan_interval ?? 10,
      tags: editingTagsQuery.data?.length ? editingTagsQuery.data : [createEmptyTag()],
    })
  }, [editForm, editingDevice, editingTagsQuery.data])

  const refreshDevices = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ['devices'] }),
      queryClient.invalidateQueries({ queryKey: ['devices', 'config-panel'] }),
      queryClient.invalidateQueries({ queryKey: ['devices', 'options'] }),
    ])
  }

  const createDeviceMutation = useMutation({
    mutationFn: async (values: DeviceEditorValues) => {
      const payload: DeviceCreatePayload = {
        name: values.name.trim(),
        type: values.type,
        host: values.host.trim(),
        port: Number(values.port),
        rack: Number(values.rack ?? 0),
        slot: Number(values.slot ?? 1),
        scan_interval: Number(values.scan_interval ?? 10),
        tags: normalizeTags(values.tags),
      }
      return devicesApi.create(payload)
    },
    onSuccess: async () => {
      message.success('设备与点位已创建')
      createForm.resetFields()
      createForm.setFieldsValue(createInitialDeviceValues())
      await refreshDevices()
    },
    onError: (error) => message.error(extractApiError(error, '创建设备失败')),
  })

  const saveDeviceMutation = useMutation({
    mutationFn: async ({ deviceId, values }: { deviceId: string; values: DeviceEditorValues }) => {
      const payload: DeviceUpdatePayload = {
        name: values.name.trim(),
        host: values.host.trim(),
        port: Number(values.port),
        rack: Number(values.rack ?? 0),
        slot: Number(values.slot ?? 1),
        scan_interval: Number(values.scan_interval ?? 10),
      }
      await devicesApi.update(deviceId, payload)
      return devicesApi.replaceTags(deviceId, normalizeTags(values.tags))
    },
    onSuccess: async () => {
      message.success('设备配置已更新')
      setEditingDevice(null)
      editForm.resetFields()
      await refreshDevices()
      await queryClient.invalidateQueries({ queryKey: ['devices', 'tags'] })
    },
    onError: (error) => message.error(extractApiError(error, '更新设备失败')),
  })

  const toggleConnectionMutation = useMutation({
    mutationFn: async ({ deviceId, online }: { deviceId: string; online: boolean }) =>
      online ? devicesApi.connect(deviceId) : devicesApi.disconnect(deviceId),
    onSuccess: async (_, variables) => {
      message.success(variables.online ? '设备已标记为在线' : '设备已标记为离线')
      await refreshDevices()
    },
    onError: (error) => message.error(extractApiError(error, '更新连接状态失败')),
  })

  const summary = useMemo(() => {
    const devices = devicesQuery.data?.devices ?? []
    return {
      total: devices.length,
      online: devices.filter((item) => item.status === 'online').length,
      mapped: devices.filter((item) => item.tag_count > 0).length,
    }
  }, [devicesQuery.data?.devices])

  const openEdit = (record: DeviceRecord) => {
    setEditingDevice(record)
  }

  const handleCreateImport = (preview: DeviceTagImportPreviewResponse) => {
    createForm.setFieldsValue({ tags: getImportedTags(preview) })
  }

  const handleEditImport = (preview: DeviceTagImportPreviewResponse) => {
    editForm.setFieldsValue({ tags: getImportedTags(preview) })
  }

  const handleCreateDevice = async () => {
    const values = await createForm.validateFields()
    createDeviceMutation.mutate(values)
  }

  const handleSaveDevice = async () => {
    if (!editingDevice) {
      return
    }
    const values = await editForm.validateFields()
    saveDeviceMutation.mutate({ deviceId: editingDevice.id, values })
  }

  const deviceColumns: TableProps<DeviceRecord>['columns'] = [
    {
      title: '设备',
      key: 'device',
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <Typography.Text strong>{record.name}</Typography.Text>
          <Typography.Text type="secondary">{record.id}</Typography.Text>
        </Space>
      ),
    },
    {
      title: '协议',
      dataIndex: 'type',
      key: 'type',
      render: (value: string) => <Tag color="blue">{value.toUpperCase()}</Tag>,
    },
    {
      title: '连接',
      key: 'endpoint',
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <Typography.Text>{record.host}:{record.port}</Typography.Text>
          <Tag color={deviceStatusMeta[record.status]?.color || 'default'}>
            {deviceStatusMeta[record.status]?.label || record.status}
          </Tag>
        </Space>
      ),
    },
    {
      title: '点位数',
      dataIndex: 'tag_count',
      key: 'tag_count',
      render: (value: number) => <Tag color={value > 0 ? 'geekblue' : 'default'}>{value}</Tag>,
    },
    {
      title: '更新时间',
      dataIndex: 'updated_at',
      key: 'updated_at',
      render: (value: string) => new Date(value).toLocaleString(),
    },
    {
      title: '操作',
      key: 'actions',
      render: (_, record) => (
        <Space wrap>
          <Button icon={<EditOutlined />} onClick={() => openEdit(record)}>
            编辑
          </Button>
          {record.status === 'online' ? (
            <Popconfirm title="将设备状态切换为离线？" onConfirm={() => toggleConnectionMutation.mutate({ deviceId: record.id, online: false })}>
              <Button icon={<DisconnectOutlined />} loading={toggleConnectionMutation.isPending}>
                置离线
              </Button>
            </Popconfirm>
          ) : (
            <Button
              type="dashed"
              icon={<CheckCircleOutlined />}
              loading={toggleConnectionMutation.isPending}
              onClick={() => toggleConnectionMutation.mutate({ deviceId: record.id, online: true })}
            >
              置在线
            </Button>
          )}
        </Space>
      ),
    },
  ]

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Alert
        type="info"
        showIcon
        message="PLC 点位现在支持正式语义字段"
        description="asset_id、point_key、deadband、debounce_ms 已经升级为正式列。这里只要配置好点位，PLC 采集就会按结构化字段喂给智能巡检。"
      />

      <Row gutter={[16, 16]}>
        <Col xs={24} md={8}>
          <Card size="small">
            <Space direction="vertical" size={2}>
              <Typography.Text type="secondary">设备总数</Typography.Text>
              <Typography.Title level={3} style={{ margin: 0 }}>{summary.total}</Typography.Title>
            </Space>
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small">
            <Space direction="vertical" size={2}>
              <Typography.Text type="secondary">在线设备</Typography.Text>
              <Typography.Title level={3} style={{ margin: 0 }}>{summary.online}</Typography.Title>
            </Space>
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small">
            <Space direction="vertical" size={2}>
              <Typography.Text type="secondary">已配置点位的设备</Typography.Text>
              <Typography.Title level={3} style={{ margin: 0 }}>{summary.mapped}</Typography.Title>
            </Space>
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col xs={24} xl={11}>
          <Card
            title={
              <Space>
                <CloudServerOutlined />
                <span>新增设备与点位</span>
              </Space>
            }
            extra={<Typography.Text type="secondary">创建时就能带上 PLC 点位语义映射</Typography.Text>}
          >
            <Form form={createForm} layout="vertical" initialValues={createInitialDeviceValues()}>
              <Row gutter={12}>
                <Col xs={24} md={12}>
                  <Form.Item label="设备名称" name="name" rules={[{ required: true, message: '请输入设备名称' }]}>
                    <Input placeholder="1# 除尘器 PLC" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item label="协议" name="type" rules={[{ required: true, message: '请选择协议' }]}>
                    <Select options={deviceTypeOptions} />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item label="主机地址" name="host" rules={[{ required: true, message: '请输入 IP 或主机名' }]}>
                    <Input placeholder="192.168.1.100" />
                  </Form.Item>
                </Col>
                <Col xs={24} md={12}>
                  <Form.Item label="端口" name="port" rules={[{ required: true, message: '请输入端口' }]}>
                    <InputNumber min={0} max={65535} style={{ width: '100%' }} />
                  </Form.Item>
                </Col>
                <Col xs={12} md={8}>
                  <Form.Item label="Rack" name="rack">
                    <InputNumber min={0} style={{ width: '100%' }} />
                  </Form.Item>
                </Col>
                <Col xs={12} md={8}>
                  <Form.Item label="Slot" name="slot">
                    <InputNumber min={0} style={{ width: '100%' }} />
                  </Form.Item>
                </Col>
                <Col xs={24} md={8}>
                  <Form.Item label="采集间隔(秒)" name="scan_interval">
                    <InputNumber min={1} max={3600} style={{ width: '100%' }} />
                  </Form.Item>
                </Col>
              </Row>

              <TagImportButton
                disabled={createDeviceMutation.isPending}
                onImported={handleCreateImport}
              />

              <DeviceTagListFields />

              <Space style={{ marginTop: 16 }} wrap>
                <Button type="primary" icon={<PlusOutlined />} loading={createDeviceMutation.isPending} onClick={() => void handleCreateDevice()}>
                  创建设备
                </Button>
                <Button onClick={() => createForm.setFieldsValue(createInitialDeviceValues())}>重置</Button>
              </Space>
            </Form>
          </Card>
        </Col>

        <Col xs={24} xl={13}>
          <Card
            title={
              <Space>
                <ApiOutlined />
                <span>设备目录</span>
              </Space>
            }
            extra={
              <Button icon={<ReloadOutlined />} onClick={() => void refreshDevices()}>
                刷新
              </Button>
            }
          >
            <Table
              rowKey="id"
              columns={deviceColumns}
              dataSource={devicesQuery.data?.devices ?? []}
              loading={devicesQuery.isLoading || toggleConnectionMutation.isPending}
              pagination={{ pageSize: 6 }}
              locale={{ emptyText: <Empty description="还没有设备，先创建一个 PLC 设备" /> }}
            />
          </Card>
        </Col>
      </Row>

      <Modal
        title={editingDevice ? `编辑设备: ${editingDevice.name}` : '编辑设备'}
        open={Boolean(editingDevice)}
        onCancel={() => {
          setEditingDevice(null)
          editForm.resetFields()
        }}
        onOk={() => void handleSaveDevice()}
        confirmLoading={saveDeviceMutation.isPending}
        width={1080}
        destroyOnClose
      >
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
          message="编辑模式会替换整台设备的点位配置"
          description="保存时会同步更新设备基础信息，并整体替换当前点位列表。建议先确认点位地址和 point_key 再提交。"
        />

        <Form form={editForm} layout="vertical">
          <Row gutter={12}>
            <Col xs={24} md={12}>
              <Form.Item label="设备名称" name="name" rules={[{ required: true, message: '请输入设备名称' }]}>
                <Input />
              </Form.Item>
            </Col>
            <Col xs={24} md={12}>
              <Form.Item label="协议" name="type">
                <Select options={deviceTypeOptions} disabled />
              </Form.Item>
            </Col>
            <Col xs={24} md={12}>
              <Form.Item label="主机地址" name="host" rules={[{ required: true, message: '请输入 IP 或主机名' }]}>
                <Input />
              </Form.Item>
            </Col>
            <Col xs={24} md={12}>
              <Form.Item label="端口" name="port" rules={[{ required: true, message: '请输入端口' }]}>
                <InputNumber min={0} max={65535} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col xs={12} md={8}>
              <Form.Item label="Rack" name="rack">
                <InputNumber min={0} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col xs={12} md={8}>
              <Form.Item label="Slot" name="slot">
                <InputNumber min={0} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col xs={24} md={8}>
              <Form.Item label="采集间隔(秒)" name="scan_interval">
                <InputNumber min={1} max={3600} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>

          {editingTagsQuery.isLoading ? (
            <Card size="small" loading />
          ) : (
            <Space direction="vertical" size={16} style={{ width: '100%' }}>
              <TagImportButton
                disabled={saveDeviceMutation.isPending}
                onImported={handleEditImport}
              />
              <DeviceTagListFields />
            </Space>
          )}
        </Form>
      </Modal>
    </Space>
  )
}

export default DeviceConfigPanel
