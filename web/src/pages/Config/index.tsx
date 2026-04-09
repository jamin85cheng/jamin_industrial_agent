import React, { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Card,
  Col,
  Form,
  Input,
  InputNumber,
  Modal,
  Row,
  Select,
  Space,
  Switch,
  Table,
  Tabs,
  Tag,
  Typography,
  message,
} from 'antd'
import type { TableProps } from 'antd'
import { EditOutlined, SaveOutlined, StopOutlined, UserAddOutlined } from '@ant-design/icons'
import {
  authApi,
  extractApiError,
  systemConfigApi,
  type AuthCreateTenantPayload,
  type AuthCreateUserPayload,
  type AuthManagedUserRecord,
  type AuthRoleRecord,
  type AuthSessionRecord,
  type AuthTenantRecord,
  type AuthUpdateTenantPayload,
  type AuthUpdateUserPayload,
  type SystemConfigPayload,
} from '../../lib/api'
import { useAuthStore } from '../../stores/auth'
import DeviceConfigPanel from './DeviceConfigPanel'

const permissionOptions = [
  { label: '用户查看', value: 'user:read' },
  { label: '用户管理', value: 'user:write' },
  { label: '设备查看', value: 'device:read' },
  { label: '设备管理', value: 'device:write' },
  { label: '数据查看', value: 'data:read' },
  { label: '告警查看', value: 'alert:read' },
  { label: '告警确认', value: 'alert:acknowledge' },
  { label: '报告查看', value: 'report:read' },
  { label: '报告导出', value: 'report:export' },
  { label: '知识库查看', value: 'knowledge:read' },
  { label: '知识库维护', value: 'knowledge:write' },
]

const tenantStatusMeta: Record<string, { color: string; label: string }> = {
  active: { color: 'green', label: '启用中' },
  suspended: { color: 'orange', label: '已暂停' },
  pending: { color: 'blue', label: '待启用' },
  expired: { color: 'red', label: '已过期' },
}

const sessionStatusMeta: Record<string, { color: string; label: string }> = {
  active: { color: 'green', label: '活跃' },
  revoked: { color: 'default', label: '已撤销' },
  expired: { color: 'red', label: '已过期' },
}

const defaultConfigValues: SystemConfigPayload = {
  basic: {
    system_name: 'Jamin Industrial Agent',
    scan_interval: 10,
    alert_suppression: 15,
  },
  plc: {
    plc_type: 's7',
    ip_address: '127.0.0.1',
    port: 102,
  },
  notifications: {
    feishu_enabled: false,
    feishu_webhook: '',
    email_enabled: false,
    smtp_server: '',
  },
}

const Config: React.FC = () => {
  const [basicForm] = Form.useForm<SystemConfigPayload['basic']>()
  const [plcForm] = Form.useForm<SystemConfigPayload['plc']>()
  const [notificationForm] = Form.useForm<SystemConfigPayload['notifications']>()
  const [tenantCreateForm] = Form.useForm<AuthCreateTenantPayload>()
  const [tenantEditForm] = Form.useForm<AuthUpdateTenantPayload>()
  const [userCreateForm] = Form.useForm<AuthCreateUserPayload>()
  const [userEditForm] = Form.useForm<AuthUpdateUserPayload>()
  const [editingUser, setEditingUser] = useState<AuthManagedUserRecord | null>(null)
  const [editingTenant, setEditingTenant] = useState<AuthTenantRecord | null>(null)
  const queryClient = useQueryClient()
  const { user } = useAuthStore()

  const isAdmin = Boolean(user?.roles.includes('admin') || user?.permissions?.includes('*'))

  const systemConfigQuery = useQuery({
    queryKey: ['system', 'config'],
    queryFn: systemConfigApi.get,
    enabled: isAdmin,
  })

  const tenantsQuery = useQuery({
    queryKey: ['auth', 'tenants'],
    queryFn: authApi.listTenants,
    enabled: isAdmin,
  })

  const rolesQuery = useQuery({
    queryKey: ['auth', 'roles'],
    queryFn: authApi.listRoles,
    enabled: isAdmin,
  })

  const usersQuery = useQuery({
    queryKey: ['auth', 'users'],
    queryFn: () => authApi.listUsers({ include_inactive: true }),
    enabled: isAdmin,
  })

  const sessionsQuery = useQuery({
    queryKey: ['auth', 'sessions'],
    queryFn: () => authApi.listSessions({ include_revoked: false }),
    enabled: isAdmin,
  })

  useEffect(() => {
    const payload = systemConfigQuery.data?.config || defaultConfigValues
    basicForm.setFieldsValue(payload.basic)
    plcForm.setFieldsValue(payload.plc)
    notificationForm.setFieldsValue(payload.notifications)
  }, [basicForm, notificationForm, plcForm, systemConfigQuery.data?.config])

  const refreshAuthQueries = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ['auth', 'users'] }),
      queryClient.invalidateQueries({ queryKey: ['auth', 'tenants'] }),
      queryClient.invalidateQueries({ queryKey: ['auth', 'roles'] }),
      queryClient.invalidateQueries({ queryKey: ['auth', 'sessions'] }),
    ])
  }

  const saveSystemConfigMutation = useMutation({
    mutationFn: (payload: SystemConfigPayload) => systemConfigApi.save(payload),
    onSuccess: async () => {
      message.success('系统配置已保存')
      await queryClient.invalidateQueries({ queryKey: ['system', 'config'] })
    },
    onError: (error) => message.error(extractApiError(error, '系统配置保存失败')),
  })

  const createTenantMutation = useMutation({
    mutationFn: authApi.createTenant,
    onSuccess: async () => {
      message.success('租户已创建')
      tenantCreateForm.resetFields()
      tenantCreateForm.setFieldsValue({ status: 'active' })
      await refreshAuthQueries()
    },
    onError: (error) => message.error(extractApiError(error, '创建租户失败')),
  })

  const updateTenantMutation = useMutation({
    mutationFn: ({ tenantId, payload }: { tenantId: string; payload: AuthUpdateTenantPayload }) =>
      authApi.updateTenant(tenantId, payload),
    onSuccess: async () => {
      message.success('租户已更新')
      setEditingTenant(null)
      tenantEditForm.resetFields()
      await refreshAuthQueries()
    },
    onError: (error) => message.error(extractApiError(error, '更新租户失败')),
  })

  const createUserMutation = useMutation({
    mutationFn: authApi.createUser,
    onSuccess: async () => {
      message.success('用户已创建')
      userCreateForm.resetFields()
      userCreateForm.setFieldsValue({
        roles: ['viewer'],
        permissions: ['report:read'],
        tenant_id: user?.tenant_id ?? 'default',
        is_active: true,
        is_demo: false,
      })
      await refreshAuthQueries()
    },
    onError: (error) => message.error(extractApiError(error, '创建用户失败')),
  })

  const updateUserMutation = useMutation({
    mutationFn: ({ userId, payload }: { userId: string; payload: AuthUpdateUserPayload }) =>
      authApi.updateUser(userId, payload),
    onSuccess: async () => {
      message.success('用户已更新')
      setEditingUser(null)
      userEditForm.resetFields()
      await refreshAuthQueries()
    },
    onError: (error) => message.error(extractApiError(error, '更新用户失败')),
  })

  const revokeSessionMutation = useMutation({
    mutationFn: (tokenId: string) => authApi.revokeSession(tokenId),
    onSuccess: async () => {
      message.success('会话已撤销')
      await refreshAuthQueries()
    },
    onError: (error) => message.error(extractApiError(error, '撤销会话失败')),
  })

  const handleSave = async () => {
    const [basicValues, plcValues, notificationValues] = await Promise.all([
      basicForm.validateFields(),
      plcForm.validateFields(),
      notificationForm.validateFields(),
    ])
    saveSystemConfigMutation.mutate({
      basic: basicValues,
      plc: plcValues,
      notifications: notificationValues,
    })
  }

  const handleValidatePlc = async () => {
    await plcForm.validateFields()
    message.success('PLC 参数格式校验通过')
  }

  const handleCreateTenant = async () => {
    const values = await tenantCreateForm.validateFields()
    createTenantMutation.mutate({
      id: values.id,
      name: values.name,
      status: values.status ?? 'active',
    })
  }

  const handleCreateUser = async () => {
    const values = await userCreateForm.validateFields()
    createUserMutation.mutate({
      user_id: values.user_id,
      username: values.username,
      password: values.password,
      roles: values.roles ?? [],
      permissions: values.permissions ?? [],
      tenant_id: values.tenant_id ?? user?.tenant_id ?? 'default',
      is_active: values.is_active ?? true,
      is_demo: values.is_demo ?? false,
    })
  }

  const openUserEdit = (record: AuthManagedUserRecord) => {
    setEditingUser(record)
    userEditForm.setFieldsValue({
      username: record.username,
      roles: record.roles,
      permissions: record.permissions,
      tenant_id: record.tenant_id,
      is_active: record.is_active,
      is_demo: record.is_demo,
    })
  }

  const openTenantEdit = (record: AuthTenantRecord) => {
    setEditingTenant(record)
    tenantEditForm.setFieldsValue({
      name: record.name,
      status: record.status,
    })
  }

  const handleUpdateUser = async () => {
    if (!editingUser) return
    const values = await userEditForm.validateFields()
    updateUserMutation.mutate({
      userId: editingUser.user_id,
      payload: values,
    })
  }

  const handleUpdateTenant = async () => {
    if (!editingTenant) return
    const values = await tenantEditForm.validateFields()
    updateTenantMutation.mutate({
      tenantId: editingTenant.id,
      payload: values,
    })
  }

  const handleToggleUser = async (record: AuthManagedUserRecord) => {
    try {
      await updateUserMutation.mutateAsync({
        userId: record.user_id,
        payload: { is_active: !record.is_active },
      })
    } catch {
      // Error is surfaced by mutation callbacks.
    }
  }

  const handleToggleTenant = async (record: AuthTenantRecord) => {
    try {
      await updateTenantMutation.mutateAsync({
        tenantId: record.id,
        payload: {
          status: record.status === 'active' ? 'suspended' : 'active',
        },
      })
    } catch {
      // Error is surfaced by mutation callbacks.
    }
  }

  const handleRevokeSession = async (record: AuthSessionRecord) => {
    try {
      await revokeSessionMutation.mutateAsync(record.token_id)
    } catch {
      // Error is surfaced by mutation callbacks.
    }
  }

  const roleOptions = (rolesQuery.data?.roles ?? []).map((role: AuthRoleRecord) => ({
    label: `${role.name} (${role.id})`,
    value: role.id,
  }))

  const userColumns: TableProps<AuthManagedUserRecord>['columns'] = [
    {
      title: '用户名',
      dataIndex: 'username',
      key: 'username',
    },
    {
      title: '租户',
      dataIndex: 'tenant_id',
      key: 'tenant_id',
      render: (tenantId: string) => <Tag color="geekblue">{tenantId}</Tag>,
    },
    {
      title: '角色',
      dataIndex: 'roles',
      key: 'roles',
      render: (roles: string[]) => (
        <Space wrap>
          {roles.length ? roles.map((role) => <Tag key={role}>{role}</Tag>) : <Tag>未分配</Tag>}
        </Space>
      ),
    },
    {
      title: '权限',
      dataIndex: 'permissions',
      key: 'permissions',
      render: (permissions: string[]) => (
        <Space wrap>
          {permissions.length ? permissions.slice(0, 4).map((permission) => <Tag key={permission}>{permission}</Tag>) : <Tag>默认</Tag>}
          {permissions.length > 4 ? <Tag>+{permissions.length - 4}</Tag> : null}
        </Space>
      ),
    },
    {
      title: '状态',
      key: 'status',
      render: (_, record) => (
        <Space wrap>
          <Tag color={record.is_active ? 'green' : 'default'}>{record.is_active ? '启用' : '停用'}</Tag>
          {record.is_demo ? <Tag color="gold">演示账号</Tag> : null}
        </Space>
      ),
    },
    {
      title: '最近登录',
      dataIndex: 'last_login_at',
      key: 'last_login_at',
      render: (value?: string | null) => (value ? new Date(value).toLocaleString() : '未登录'),
    },
    {
      title: '操作',
      key: 'actions',
      render: (_, record) => (
        <Space>
          <Button icon={<EditOutlined />} onClick={() => openUserEdit(record)}>
            编辑
          </Button>
          <Button icon={<StopOutlined />} danger={record.is_active} onClick={() => void handleToggleUser(record)}>
            {record.is_active ? '停用' : '启用'}
          </Button>
        </Space>
      ),
    },
  ]

  const tenantColumns: TableProps<AuthTenantRecord>['columns'] = [
    {
      title: '租户标识',
      dataIndex: 'id',
      key: 'id',
    },
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => {
        const meta = tenantStatusMeta[status] ?? { color: 'default', label: status }
        return <Tag color={meta.color}>{meta.label}</Tag>
      },
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (value?: string | null) => (value ? new Date(value).toLocaleString() : '-'),
    },
    {
      title: '操作',
      key: 'actions',
      render: (_, record) => (
        <Space>
          <Button icon={<EditOutlined />} onClick={() => openTenantEdit(record)}>
            编辑
          </Button>
          <Button
            icon={<StopOutlined />}
            danger={record.status === 'active'}
            disabled={record.id === 'default' && record.status === 'active'}
            onClick={() => void handleToggleTenant(record)}
          >
            {record.status === 'active' ? '暂停' : '启用'}
          </Button>
        </Space>
      ),
    },
  ]

  const sessionColumns: TableProps<AuthSessionRecord>['columns'] = [
    {
      title: 'Session ID',
      dataIndex: 'token_id',
      key: 'token_id',
      render: (tokenId: string) => <Typography.Text code>{tokenId.slice(0, 12)}...</Typography.Text>,
    },
    {
      title: '用户',
      key: 'username',
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <span>{record.username}</span>
          <Typography.Text type="secondary">{record.user_id}</Typography.Text>
        </Space>
      ),
    },
    {
      title: '租户',
      dataIndex: 'tenant_id',
      key: 'tenant_id',
      render: (tenantId: string) => <Tag color="geekblue">{tenantId}</Tag>,
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => {
        const meta = sessionStatusMeta[status] ?? { color: 'default', label: status }
        return <Tag color={meta.color}>{meta.label}</Tag>
      },
    },
    {
      title: '最近使用',
      dataIndex: 'last_used_at',
      key: 'last_used_at',
      render: (value?: string | null) => (value ? new Date(value).toLocaleString() : '未使用'),
    },
    {
      title: '过期时间',
      dataIndex: 'expires_at',
      key: 'expires_at',
      render: (value?: string | null) => (value ? new Date(value).toLocaleString() : '-'),
    },
    {
      title: '操作',
      key: 'actions',
      render: (_, record) => (
        <Button danger disabled={record.status !== 'active'} onClick={() => void handleRevokeSession(record)}>
          撤销会话
        </Button>
      ),
    },
  ]

  if (!isAdmin) {
    return (
      <Card title="系统配置">
        <Alert
          type="warning"
          showIcon
          message="当前账号没有管理员权限"
          description="系统配置、租户管理、用户管理和会话治理仅对管理员开放。"
        />
      </Card>
    )
  }

  return (
    <Card
      title="系统配置"
      extra={
        <Typography.Text type="secondary">
          配置来源：{systemConfigQuery.data?.source || 'defaults'}
          {systemConfigQuery.data?.updated_at ? ` · 最近更新 ${new Date(systemConfigQuery.data.updated_at).toLocaleString()}` : ''}
        </Typography.Text>
      }
    >
      <Tabs
        defaultActiveKey="basic"
        items={[
          {
            key: 'basic',
            label: '基础配置',
            children: (
              <Form form={basicForm} layout="vertical" style={{ maxWidth: 640 }} initialValues={defaultConfigValues.basic}>
                <Form.Item label="系统名称" name="system_name" rules={[{ required: true, message: '请输入系统名称' }]}>
                  <Input />
                </Form.Item>
                <Form.Item label="采集间隔（秒）" name="scan_interval" rules={[{ required: true, message: '请输入采集间隔' }]}>
                  <InputNumber min={1} style={{ width: '100%' }} />
                </Form.Item>
                <Form.Item label="告警抑制（分钟）" name="alert_suppression" rules={[{ required: true, message: '请输入告警抑制时间' }]}>
                  <InputNumber min={0} style={{ width: '100%' }} />
                </Form.Item>
                <Form.Item>
                  <Button type="primary" icon={<SaveOutlined />} loading={saveSystemConfigMutation.isPending} onClick={() => void handleSave()}>
                    保存配置
                  </Button>
                </Form.Item>
              </Form>
            ),
          },
          {
            key: 'plc',
            label: 'PLC 配置',
            children: (
              <Form form={plcForm} layout="vertical" style={{ maxWidth: 640 }} initialValues={defaultConfigValues.plc}>
                <Form.Item label="PLC 类型" name="plc_type" rules={[{ required: true, message: '请选择 PLC 类型' }]}>
                  <Select
                    options={[
                      { value: 's7', label: '西门子 S7' },
                      { value: 'modbus', label: 'Modbus TCP' },
                    ]}
                  />
                </Form.Item>
                <Form.Item label="IP 地址" name="ip_address" rules={[{ required: true, message: '请输入 IP 地址' }]}>
                  <Input />
                </Form.Item>
                <Form.Item label="端口" name="port" rules={[{ required: true, message: '请输入端口' }]}>
                  <InputNumber min={1} max={65535} style={{ width: '100%' }} />
                </Form.Item>
                <Form.Item>
                  <Space wrap>
                    <Button type="primary" icon={<SaveOutlined />} loading={saveSystemConfigMutation.isPending} onClick={() => void handleSave()}>
                      保存 PLC 配置
                    </Button>
                    <Button onClick={() => void handleValidatePlc()}>
                      校验参数
                    </Button>
                  </Space>
                </Form.Item>
              </Form>
            ),
          },
          {
            key: 'notification',
            label: '通知配置',
            children: (
              <Form form={notificationForm} layout="vertical" style={{ maxWidth: 640 }} initialValues={defaultConfigValues.notifications}>
                <Form.Item label="启用飞书通知" name="feishu_enabled" valuePropName="checked">
                  <Switch />
                </Form.Item>
                <Form.Item label="飞书 Webhook" name="feishu_webhook">
                  <Input.TextArea rows={3} placeholder="请输入飞书机器人 Webhook 地址" />
                </Form.Item>
                <Form.Item label="启用邮件通知" name="email_enabled" valuePropName="checked">
                  <Switch />
                </Form.Item>
                <Form.Item label="SMTP 服务器" name="smtp_server">
                  <Input placeholder="smtp.example.com" />
                </Form.Item>
                <Form.Item>
                  <Button type="primary" icon={<SaveOutlined />} loading={saveSystemConfigMutation.isPending} onClick={() => void handleSave()}>
                    保存通知配置
                  </Button>
                </Form.Item>
              </Form>
            ),
          },
          {
            key: 'devices',
            label: '设备与点位',
            children: <DeviceConfigPanel />,
          },
          {
            key: 'auth',
            label: '身份与租户',
            children: (
              <Space direction="vertical" size={16} style={{ width: '100%' }}>
                <Typography.Paragraph type="secondary" style={{ marginBottom: 0 }}>
                  当前已经支持租户、角色、用户和 refresh session 的日常管理，后续会继续补齐角色矩阵编辑和更细的审计视图。
                </Typography.Paragraph>

                <Row gutter={[16, 16]}>
                  <Col xs={24} xl={10}>
                    <Card title="新建租户">
                      <Form form={tenantCreateForm} layout="vertical" initialValues={{ status: 'active' }}>
                        <Form.Item name="id" label="租户标识" rules={[{ required: true, message: '请输入租户标识' }]}>
                          <Input placeholder="例如 tenant-shanghai" />
                        </Form.Item>
                        <Form.Item name="name" label="租户名称" rules={[{ required: true, message: '请输入租户名称' }]}>
                          <Input placeholder="例如 上海工厂" />
                        </Form.Item>
                        <Form.Item name="status" label="状态">
                          <Select
                            options={[
                              { label: '启用中', value: 'active' },
                              { label: '待启用', value: 'pending' },
                              { label: '已暂停', value: 'suspended' },
                              { label: '已过期', value: 'expired' },
                            ]}
                          />
                        </Form.Item>
                        <Form.Item>
                          <Button type="primary" icon={<UserAddOutlined />} loading={createTenantMutation.isPending} onClick={() => void handleCreateTenant()}>
                            创建租户
                          </Button>
                        </Form.Item>
                      </Form>
                    </Card>
                  </Col>

                  <Col xs={24} xl={14}>
                    <Card title="租户列表">
                      <Table rowKey="id" columns={tenantColumns} dataSource={tenantsQuery.data?.tenants ?? []} loading={tenantsQuery.isLoading || updateTenantMutation.isPending} pagination={{ pageSize: 5 }} />
                    </Card>
                  </Col>

                  <Col xs={24} xl={10}>
                    <Card title="新建用户">
                      <Form
                        form={userCreateForm}
                        layout="vertical"
                        initialValues={{
                          roles: ['viewer'],
                          permissions: ['report:read'],
                          tenant_id: user?.tenant_id ?? 'default',
                          is_active: true,
                          is_demo: false,
                        }}
                      >
                        <Form.Item name="user_id" label="用户 ID">
                          <Input placeholder="留空时自动生成" />
                        </Form.Item>
                        <Form.Item name="username" label="用户名" rules={[{ required: true, message: '请输入用户名' }]}>
                          <Input placeholder="例如 ops.shanghai" />
                        </Form.Item>
                        <Form.Item name="password" label="初始密码" rules={[{ required: true, message: '请输入初始密码' }]}>
                          <Input.Password placeholder="至少 8 位" />
                        </Form.Item>
                        <Form.Item name="tenant_id" label="所属租户" rules={[{ required: true, message: '请选择租户' }]}>
                          <Select
                            options={(tenantsQuery.data?.tenants ?? []).map((tenant) => ({
                              label: `${tenant.name} (${tenant.id})`,
                              value: tenant.id,
                            }))}
                          />
                        </Form.Item>
                        <Form.Item name="roles" label="角色">
                          <Select mode="multiple" options={roleOptions} loading={rolesQuery.isLoading} placeholder="选择角色" />
                        </Form.Item>
                        <Form.Item name="permissions" label="附加权限">
                          <Select mode="multiple" options={permissionOptions} />
                        </Form.Item>
                        <Form.Item name="is_active" label="立即启用" valuePropName="checked">
                          <Switch />
                        </Form.Item>
                        <Form.Item name="is_demo" label="标记为演示账号" valuePropName="checked">
                          <Switch />
                        </Form.Item>
                        <Form.Item>
                          <Button type="primary" icon={<UserAddOutlined />} loading={createUserMutation.isPending} onClick={() => void handleCreateUser()}>
                            创建用户
                          </Button>
                        </Form.Item>
                      </Form>
                    </Card>
                  </Col>

                  <Col xs={24} xl={14}>
                    <Card title="用户列表">
                      <Table rowKey="user_id" columns={userColumns} dataSource={usersQuery.data?.users ?? []} loading={usersQuery.isLoading || updateUserMutation.isPending} pagination={{ pageSize: 5 }} />
                    </Card>
                  </Col>

                  <Col xs={24}>
                    <Card
                      title="活跃会话"
                      extra={<Typography.Text type="secondary">这里展示当前保存的 refresh session</Typography.Text>}
                    >
                      <Table rowKey="token_id" columns={sessionColumns} dataSource={sessionsQuery.data?.sessions ?? []} loading={sessionsQuery.isLoading || revokeSessionMutation.isPending} pagination={{ pageSize: 5 }} />
                    </Card>
                  </Col>
                </Row>

                <Modal
                  title={editingUser ? `编辑用户：${editingUser.username}` : '编辑用户'}
                  open={Boolean(editingUser)}
                  onCancel={() => {
                    setEditingUser(null)
                    userEditForm.resetFields()
                  }}
                  onOk={() => void handleUpdateUser()}
                  confirmLoading={updateUserMutation.isPending}
                  destroyOnClose
                >
                  <Form form={userEditForm} layout="vertical">
                    <Form.Item name="username" label="用户名" rules={[{ required: true, message: '请输入用户名' }]}>
                      <Input />
                    </Form.Item>
                    <Form.Item name="tenant_id" label="所属租户" rules={[{ required: true, message: '请选择租户' }]}>
                      <Select
                        options={(tenantsQuery.data?.tenants ?? []).map((tenant) => ({
                          label: `${tenant.name} (${tenant.id})`,
                          value: tenant.id,
                        }))}
                      />
                    </Form.Item>
                    <Form.Item name="roles" label="角色">
                      <Select mode="multiple" options={roleOptions} />
                    </Form.Item>
                    <Form.Item name="permissions" label="附加权限">
                      <Select mode="multiple" options={permissionOptions} />
                    </Form.Item>
                    <Form.Item name="is_active" label="启用" valuePropName="checked">
                      <Switch />
                    </Form.Item>
                    <Form.Item name="is_demo" label="演示账号" valuePropName="checked">
                      <Switch />
                    </Form.Item>
                  </Form>
                </Modal>

                <Modal
                  title={editingTenant ? `编辑租户：${editingTenant.name}` : '编辑租户'}
                  open={Boolean(editingTenant)}
                  onCancel={() => {
                    setEditingTenant(null)
                    tenantEditForm.resetFields()
                  }}
                  onOk={() => void handleUpdateTenant()}
                  confirmLoading={updateTenantMutation.isPending}
                  destroyOnClose
                >
                  <Form form={tenantEditForm} layout="vertical">
                    <Form.Item name="name" label="租户名称" rules={[{ required: true, message: '请输入租户名称' }]}>
                      <Input />
                    </Form.Item>
                    <Form.Item name="status" label="状态">
                      <Select
                        options={[
                          { label: '启用中', value: 'active' },
                          { label: '待启用', value: 'pending' },
                          { label: '已暂停', value: 'suspended' },
                          { label: '已过期', value: 'expired' },
                        ]}
                      />
                    </Form.Item>
                  </Form>
                </Modal>
              </Space>
            ),
          },
        ]}
      />
    </Card>
  )
}

export default Config
