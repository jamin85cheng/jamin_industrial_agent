import React from 'react'
import { Card, Form, Input, Button, Switch, Select, Tabs, message, InputNumber } from 'antd'
import { SaveOutlined } from '@ant-design/icons'

const Config: React.FC = () => {
  const [basicForm] = Form.useForm()
  const [plcForm] = Form.useForm()
  const [notificationForm] = Form.useForm()

  const handleSave = async () => {
    await basicForm.validateFields()
    await plcForm.validateFields()
    await notificationForm.validateFields()
    message.success('配置已保存')
  }

  return (
    <Card title="系统配置">
      <Tabs
        defaultActiveKey="basic"
        items={[
          {
            key: 'basic',
            label: '基础配置',
            children: (
              <Form
                form={basicForm}
                layout="vertical"
                style={{ maxWidth: 600 }}
                initialValues={{
                  system_name: 'Jamin Industrial Agent',
                  scan_interval: 10,
                  alert_suppression: 15,
                }}
              >
                <Form.Item
                  label="系统名称"
                  name="system_name"
                  rules={[{ required: true, message: '请输入系统名称' }]}
                >
                  <Input />
                </Form.Item>
                <Form.Item
                  label="采集间隔（秒）"
                  name="scan_interval"
                  rules={[{ required: true, message: '请输入采集间隔' }]}
                >
                  <InputNumber min={1} style={{ width: '100%' }} />
                </Form.Item>
                <Form.Item
                  label="告警抑制（分钟）"
                  name="alert_suppression"
                  rules={[{ required: true, message: '请输入抑制时间' }]}
                >
                  <InputNumber min={0} style={{ width: '100%' }} />
                </Form.Item>
                <Form.Item>
                  <Button type="primary" icon={<SaveOutlined />} onClick={() => void handleSave()}>
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
              <Form
                form={plcForm}
                layout="vertical"
                style={{ maxWidth: 600 }}
                initialValues={{
                  plc_type: 's7',
                  ip_address: '192.168.1.100',
                  port: 102,
                }}
              >
                <Form.Item
                  label="PLC 类型"
                  name="plc_type"
                  rules={[{ required: true, message: '请选择 PLC 类型' }]}
                >
                  <Select
                    options={[
                      { value: 's7', label: '西门子 S7' },
                      { value: 'modbus', label: 'Modbus TCP' },
                    ]}
                  />
                </Form.Item>
                <Form.Item
                  label="IP 地址"
                  name="ip_address"
                  rules={[{ required: true, message: '请输入 IP 地址' }]}
                >
                  <Input />
                </Form.Item>
                <Form.Item
                  label="端口"
                  name="port"
                  rules={[{ required: true, message: '请输入端口' }]}
                >
                  <InputNumber min={1} max={65535} style={{ width: '100%' }} />
                </Form.Item>
                <Form.Item>
                  <Button type="primary" onClick={() => message.success('连接测试通过（模拟）')}>
                    测试连接
                  </Button>
                </Form.Item>
              </Form>
            ),
          },
          {
            key: 'notification',
            label: '通知配置',
            children: (
              <Form
                form={notificationForm}
                layout="vertical"
                style={{ maxWidth: 600 }}
                initialValues={{
                  feishu_enabled: false,
                  email_enabled: false,
                }}
              >
                <Form.Item label="启用飞书通知" name="feishu_enabled" valuePropName="checked">
                  <Switch />
                </Form.Item>
                <Form.Item label="飞书 Webhook" name="feishu_webhook">
                  <Input.TextArea placeholder="输入飞书机器人 Webhook 地址" rows={3} />
                </Form.Item>
                <Form.Item label="启用邮件通知" name="email_enabled" valuePropName="checked">
                  <Switch />
                </Form.Item>
                <Form.Item label="SMTP 服务器" name="smtp_server">
                  <Input placeholder="smtp.example.com" />
                </Form.Item>
              </Form>
            ),
          },
        ]}
      />
    </Card>
  )
}

export default Config
