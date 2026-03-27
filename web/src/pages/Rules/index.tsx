import React, { useState } from 'react'
import {
  Card,
  Table,
  Button,
  Tag,
  Space,
  Modal,
  Form,
  Input,
  Select,
  Switch,
  message,
  Popconfirm,
  InputNumber,
  Empty,
} from 'antd'
import { PlusOutlined, EditOutlined, DeleteOutlined } from '@ant-design/icons'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { TableProps } from 'antd'
import { extractApiError, rulesApi, type AlertRule } from '../../lib/api'

type Severity = AlertRule['severity']

interface RuleFormValues {
  name: string
  severity: Severity
  message: string
  tag: string
  operator: '>' | '>=' | '<' | '<='
  value: number
  suppression_window_minutes: number
  enabled: boolean
}

const severityMeta: Record<Severity, { color: string; label: string }> = {
  critical: { color: 'red', label: '严重' },
  warning: { color: 'orange', label: '警告' },
  info: { color: 'blue', label: '提示' },
}

const buildRulePayload = (values: RuleFormValues, ruleId?: string): AlertRule => ({
  rule_id: ruleId ?? `RULE_${Date.now().toString().slice(-8)}`,
  name: values.name,
  enabled: values.enabled,
  condition: {
    type: 'threshold',
    tag: values.tag,
    operator: values.operator,
    value: values.value,
  },
  severity: values.severity,
  message: values.message,
  suppression_window_minutes: values.suppression_window_minutes,
})

const Rules: React.FC = () => {
  const [isModalVisible, setIsModalVisible] = useState(false)
  const [editingRule, setEditingRule] = useState<AlertRule | null>(null)
  const [form] = Form.useForm<RuleFormValues>()
  const queryClient = useQueryClient()

  const rulesQuery = useQuery({
    queryKey: ['rules'],
    queryFn: rulesApi.list,
  })

  const createMutation = useMutation({
    mutationFn: rulesApi.create,
    onSuccess: () => {
      message.success('规则已创建')
      void queryClient.invalidateQueries({ queryKey: ['rules'] })
    },
    onError: (error) => message.error(extractApiError(error, '创建规则失败')),
  })

  const updateMutation = useMutation({
    mutationFn: ({ ruleId, payload }: { ruleId: string; payload: AlertRule }) =>
      rulesApi.update(ruleId, payload),
    onSuccess: () => {
      message.success('规则已更新')
      void queryClient.invalidateQueries({ queryKey: ['rules'] })
    },
    onError: (error) => message.error(extractApiError(error, '更新规则失败')),
  })

  const deleteMutation = useMutation({
    mutationFn: rulesApi.remove,
    onSuccess: () => {
      message.success('规则已删除')
      void queryClient.invalidateQueries({ queryKey: ['rules'] })
    },
    onError: (error) => message.error(extractApiError(error, '删除规则失败')),
  })

  const columns: TableProps<AlertRule>['columns'] = [
    {
      title: '规则 ID',
      dataIndex: 'rule_id',
      key: 'rule_id',
    },
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: '触发条件',
      key: 'condition',
      render: (_, record) =>
        `${record.condition.tag ?? '未命名测点'} ${record.condition.operator ?? ''} ${record.condition.value ?? ''}`,
    },
    {
      title: '消息',
      dataIndex: 'message',
      key: 'message',
      ellipsis: true,
    },
    {
      title: '级别',
      dataIndex: 'severity',
      key: 'severity',
      render: (severity: Severity) => {
        const meta = severityMeta[severity]
        return <Tag color={meta.color}>{meta.label}</Tag>
      },
    },
    {
      title: '状态',
      dataIndex: 'enabled',
      key: 'enabled',
      render: (enabled: boolean, record) => (
        <Switch
          checked={enabled}
          size="small"
          onChange={(checked) => {
            const payload = {
              ...record,
              enabled: checked,
            }
            updateMutation.mutate({ ruleId: record.rule_id, payload })
          }}
        />
      ),
    },
    {
      title: '操作',
      key: 'action',
      render: (_, record) => (
        <Space>
          <Button
            type="text"
            icon={<EditOutlined />}
            onClick={() => {
              setEditingRule(record)
              setIsModalVisible(true)
              form.setFieldsValue({
                name: record.name,
                severity: record.severity,
                message: record.message,
                tag: record.condition.tag ?? '',
                operator: (record.condition.operator as RuleFormValues['operator']) ?? '>',
                value: record.condition.value ?? 0,
                suppression_window_minutes: record.suppression_window_minutes,
                enabled: record.enabled,
              })
            }}
          />
          <Popconfirm
            title={`确认删除规则 ${record.name} 吗？`}
            onConfirm={() => deleteMutation.mutate(record.rule_id)}
          >
            <Button type="text" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      ),
    },
  ]

  const handleAdd = () => {
    setEditingRule(null)
    setIsModalVisible(true)
    form.setFieldsValue({
      severity: 'warning',
      operator: '>',
      value: 0,
      suppression_window_minutes: 15,
      enabled: true,
    })
  }

  const handleSubmit = async () => {
    const values = await form.validateFields()
    const payload = buildRulePayload(values, editingRule?.rule_id)

    if (editingRule) {
      updateMutation.mutate({ ruleId: editingRule.rule_id, payload })
    } else {
      createMutation.mutate(payload)
    }

    setIsModalVisible(false)
    setEditingRule(null)
    form.resetFields()
  }

  return (
    <div>
      <Card
        title="规则管理"
        extra={
          <Button type="primary" icon={<PlusOutlined />} onClick={handleAdd}>
            新建规则
          </Button>
        }
      >
        <Table
          columns={columns}
          dataSource={rulesQuery.data ?? []}
          rowKey="rule_id"
          loading={rulesQuery.isLoading}
          pagination={{ pageSize: 6 }}
          locale={{ emptyText: <Empty description="暂无规则数据" /> }}
        />
      </Card>

      <Modal
        title={editingRule ? '编辑规则' : '新建规则'}
        open={isModalVisible}
        onOk={() => void handleSubmit()}
        onCancel={() => {
          setIsModalVisible(false)
          setEditingRule(null)
          form.resetFields()
        }}
        confirmLoading={createMutation.isPending || updateMutation.isPending}
        destroyOnClose
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="name"
            label="规则名称"
            rules={[{ required: true, message: '请输入规则名称' }]}
          >
            <Input placeholder="例如：缺氧异常" />
          </Form.Item>
          <Form.Item
            name="message"
            label="告警消息"
            rules={[{ required: true, message: '请输入告警消息' }]}
          >
            <Input.TextArea placeholder="请输入触发后的提示信息" rows={3} />
          </Form.Item>
          <Form.Item
            name="severity"
            label="级别"
            rules={[{ required: true, message: '请选择规则级别' }]}
          >
            <Select
              options={[
                { value: 'critical', label: '严重' },
                { value: 'warning', label: '警告' },
                { value: 'info', label: '提示' },
              ]}
            />
          </Form.Item>
          <Form.Item
            name="tag"
            label="测点名称"
            rules={[{ required: true, message: '请输入测点名称' }]}
          >
            <Input placeholder="例如：DO / pressure / temperature" />
          </Form.Item>
          <Space style={{ display: 'flex' }} align="baseline">
            <Form.Item
              name="operator"
              label="比较符"
              rules={[{ required: true, message: '请选择比较符' }]}
              style={{ flex: 1 }}
            >
              <Select
                options={[
                  { value: '>', label: '>' },
                  { value: '>=', label: '>=' },
                  { value: '<', label: '<' },
                  { value: '<=', label: '<=' },
                ]}
              />
            </Form.Item>
            <Form.Item
              name="value"
              label="阈值"
              rules={[{ required: true, message: '请输入阈值' }]}
              style={{ flex: 1 }}
            >
              <InputNumber style={{ width: '100%' }} />
            </Form.Item>
          </Space>
          <Form.Item
            name="suppression_window_minutes"
            label="抑制窗口（分钟）"
            rules={[{ required: true, message: '请输入抑制窗口' }]}
          >
            <InputNumber min={0} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="enabled" label="启用规则" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}

export default Rules
