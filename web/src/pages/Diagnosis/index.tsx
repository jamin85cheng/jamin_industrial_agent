import React, { useState } from 'react'
import { Card, Input, Button, List, Avatar, Tag, Typography, Space, Select, Empty, message } from 'antd'
import { RobotOutlined, UserOutlined, FileTextOutlined } from '@ant-design/icons'
import { useMutation, useQuery } from '@tanstack/react-query'
import { devicesApi, extractApiError, knowledgeApi } from '../../lib/api'

const { Text, Paragraph } = Typography
const { TextArea } = Input

interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  timestamp: string
}

interface DiagnosisHistoryItem {
  id: string
  title: string
  date: string
  status: '已完成'
}

const initialMessages: ChatMessage[] = [
  {
    id: '1',
    role: 'assistant',
    content:
      '您好，我是工业智能诊断助手。请输入故障现象，并可选关联设备，我会调用当前后端诊断接口返回根因和建议。',
    timestamp: '2026-03-27 09:30:00',
  },
]

const Diagnosis: React.FC = () => {
  const [input, setInput] = useState('')
  const [selectedDeviceId, setSelectedDeviceId] = useState<string>()
  const [messages, setMessages] = useState<ChatMessage[]>(initialMessages)
  const [historyItems, setHistoryItems] = useState<DiagnosisHistoryItem[]>([])

  const devicesQuery = useQuery({
    queryKey: ['devices', 'options'],
    queryFn: devicesApi.list,
  })

  const diagnoseMutation = useMutation({
    mutationFn: knowledgeApi.diagnose,
    onSuccess: (result, variables) => {
      const references = result.references.map((item) => `- ${item.title}`).join('\n')
      const suggestions = result.suggestions.map((item, index) => `${index + 1}. ${item}`).join('\n')
      const spareParts = result.spare_parts
        .map((item) => {
          const name = typeof item.name === 'string' ? item.name : '未命名备件'
          const quantity = typeof item.quantity === 'number' || typeof item.quantity === 'string' ? item.quantity : '-'
          return `- ${name} x ${quantity}`
        })
        .join('\n')

      const assistantMessage: ChatMessage = {
        id: result.diagnosis_id,
        role: 'assistant',
        content: `根因判断：${result.root_cause}

置信度：${Math.round(result.confidence * 100)}%

建议动作：
${suggestions || '1. 暂无建议'}

备件建议：
${spareParts || '- 暂无'}

参考文档：
${references || '- 暂无'}`,
        timestamp: new Date().toLocaleString(),
      }

      setMessages((prev) => [...prev, assistantMessage])
      setHistoryItems((prev) => [
        {
          id: result.diagnosis_id,
          title: variables.symptoms.slice(0, 24),
          date: new Date().toLocaleDateString(),
          status: '已完成',
        },
        ...prev,
      ])
    },
    onError: (error) => {
      message.error(extractApiError(error, '诊断请求失败'))
    },
  })

  const handleSend = async () => {
    if (!input.trim()) return

    const userMessage: ChatMessage = {
      id: Date.now().toString(),
      role: 'user',
      content: input,
      timestamp: new Date().toLocaleString(),
    }

    setMessages((prev) => [...prev, userMessage])
    const currentInput = input
    setInput('')

    diagnoseMutation.mutate({
      symptoms: currentInput,
      device_id: selectedDeviceId,
    })
  }

  return (
    <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
      <Card
        title={
          <span>
            <RobotOutlined /> 智能诊断
          </span>
        }
        style={{ flex: 1, minWidth: 320 }}
        styles={{ body: { padding: 0 } }}
      >
        <div style={{ display: 'flex', flexDirection: 'column', minHeight: 560 }}>
          <List
            style={{ flex: 1, overflow: 'auto', padding: 16 }}
            dataSource={messages}
            renderItem={(msg) => (
              <List.Item
                style={{
                  justifyContent: msg.role === 'user' ? 'flex-end' : 'flex-start',
                  padding: '8px 0',
                }}
              >
                <Space align="start">
                  {msg.role === 'assistant' && (
                    <Avatar icon={<RobotOutlined />} style={{ backgroundColor: '#1890ff' }} />
                  )}
                  <div
                    style={{
                      maxWidth: 560,
                      padding: 12,
                      borderRadius: 8,
                      backgroundColor: msg.role === 'user' ? '#1890ff' : '#f5f7fa',
                      color: msg.role === 'user' ? '#fff' : 'inherit',
                    }}
                  >
                    <Paragraph style={{ margin: 0, whiteSpace: 'pre-wrap' }}>
                      {msg.content}
                    </Paragraph>
                    <Text type="secondary" style={{ fontSize: 12, opacity: 0.75 }}>
                      {msg.timestamp}
                    </Text>
                  </div>
                  {msg.role === 'user' && (
                    <Avatar icon={<UserOutlined />} style={{ backgroundColor: '#52c41a' }} />
                  )}
                </Space>
              </List.Item>
            )}
          />

          <div style={{ padding: 16, borderTop: '1px solid #f0f0f0', display: 'flex', flexDirection: 'column', gap: 12 }}>
            <Select
              allowClear
              placeholder="可选：关联设备"
              value={selectedDeviceId}
              onChange={(value) => setSelectedDeviceId(value)}
              loading={devicesQuery.isLoading}
              options={(devicesQuery.data?.devices ?? []).map((device) => ({
                label: device.name,
                value: device.id,
              }))}
            />
            <Space.Compact style={{ width: '100%' }}>
              <TextArea
                value={input}
                onChange={(e) => setInput(e.target.value)}
                placeholder="描述故障现象、时间、设备名称或工艺背景..."
                autoSize={{ minRows: 2, maxRows: 4 }}
                onPressEnter={(e) => {
                  if (!e.shiftKey) {
                    e.preventDefault()
                    void handleSend()
                  }
                }}
              />
              <Button type="primary" onClick={() => void handleSend()} loading={diagnoseMutation.isPending}>
                发送
              </Button>
            </Space.Compact>
          </div>
        </div>
      </Card>

      <Card
        title={
          <span>
            <FileTextOutlined /> 诊断历史
          </span>
        }
        style={{ width: 320, flex: '0 0 320px' }}
      >
        <List
          size="small"
          locale={{ emptyText: <Empty description="本次会话还没有诊断记录" /> }}
          dataSource={historyItems}
          renderItem={(item) => (
            <List.Item>
              <List.Item.Meta
                title={item.title}
                description={
                  <Space>
                    <Text type="secondary">{item.date}</Text>
                    <Tag color="success">{item.status}</Tag>
                  </Space>
                }
              />
            </List.Item>
          )}
        />
      </Card>
    </div>
  )
}

export default Diagnosis
