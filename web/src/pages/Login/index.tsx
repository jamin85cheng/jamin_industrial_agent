import React, { useState } from 'react'
import { Form, Input, Button, Card, Typography, message } from 'antd'
import { UserOutlined, LockOutlined } from '@ant-design/icons'
import { useAuthStore } from '../../stores/auth'
import { authApi, extractApiError } from '../../lib/api'
import './style.css'

interface LoginFormValues {
  username: string
  password: string
}

const Login: React.FC = () => {
  const [loading, setLoading] = useState(false)
  const { login, updateUser } = useAuthStore()

  const handleSubmit = async (values: LoginFormValues) => {
    setLoading(true)

    try {
      const response = await authApi.login(values)
      login(
        response.token,
        response.refreshToken,
        response.user,
        response.expiresIn,
        response.refreshExpiresIn,
      )

      try {
        const profile = await authApi.me()
        updateUser({
          ...profile,
          role: profile.roles?.[0] ?? response.user.role,
        })
      } catch {
        // Keep the session available even if profile hydration is temporarily unavailable.
      }

      message.success(`欢迎回来，${response.user.username}`)
    } catch (error) {
      message.error(extractApiError(error, '登录失败，请稍后重试'))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="login-page">
      <div className="login-container">
        <Card className="login-card">
          <div className="login-header">
            <div className="login-logo">JIA</div>
            <h1 className="login-title">Jamin Industrial Agent</h1>
            <p className="login-subtitle">工业智能监控与诊断平台</p>
          </div>

          <Form name="login" onFinish={handleSubmit} autoComplete="off" size="large">
            <Form.Item
              name="username"
              rules={[{ required: true, message: '请输入用户名' }]}
            >
              <Input prefix={<UserOutlined />} placeholder="用户名" />
            </Form.Item>

            <Form.Item
              name="password"
              rules={[{ required: true, message: '请输入密码' }]}
            >
              <Input.Password prefix={<LockOutlined />} placeholder="密码" />
            </Form.Item>

            <Form.Item>
              <Button type="primary" htmlType="submit" loading={loading} block>
                登录
              </Button>
            </Form.Item>
          </Form>

          <div className="login-tips">
            <Typography.Paragraph style={{ marginBottom: 4 }}>
              开发环境演示账号：admin / admin123
            </Typography.Paragraph>
            <Typography.Paragraph style={{ marginBottom: 4 }}>
              开发环境操作员：operator / operator123
            </Typography.Paragraph>
            <Typography.Paragraph style={{ marginBottom: 0 }}>
              开发环境只读用户：viewer / viewer123
            </Typography.Paragraph>
          </div>
        </Card>
      </div>
    </div>
  )
}

export default Login
