import React, { useState } from 'react'
import { Layout, Menu, theme, Badge, Avatar, Dropdown, Button, Typography, message } from 'antd'
import {
  DashboardOutlined,
  AlertOutlined,
  RobotOutlined,
  FileTextOutlined,
  SettingOutlined,
  BellOutlined,
  UserOutlined,
  LogoutOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
} from '@ant-design/icons'
import type { MenuProps } from 'antd'
import { useNavigate, useLocation } from 'react-router-dom'
import { useAuthStore } from '../../stores/auth'

const { Header, Sider } = Layout
const { Text } = Typography

interface MainLayoutProps {
  children: React.ReactNode
}

const menuItems: MenuProps['items'] = [
  {
    key: '/dashboard',
    icon: <DashboardOutlined />,
    label: '运营总览',
  },
  {
    key: '/alerts',
    icon: <AlertOutlined />,
    label: (
      <span>
        告警中心
        <Badge count={3} size="small" style={{ marginLeft: 8 }} />
      </span>
    ),
  },
  {
    key: '/diagnosis',
    icon: <RobotOutlined />,
    label: '智能诊断',
  },
  {
    key: '/rules',
    icon: <FileTextOutlined />,
    label: '规则管理',
  },
  {
    key: '/config',
    icon: <SettingOutlined />,
    label: '系统配置',
  },
]

const userMenuItems: MenuProps['items'] = [
  {
    key: 'profile',
    icon: <UserOutlined />,
    label: '个人中心',
  },
  {
    key: 'logout',
    icon: <LogoutOutlined />,
    label: '退出登录',
  },
]

const MainLayout: React.FC<MainLayoutProps> = ({ children }) => {
  const [collapsed, setCollapsed] = useState(false)
  const navigate = useNavigate()
  const location = useLocation()
  const { user, logout } = useAuthStore()

  const {
    token: { colorBgContainer },
  } = theme.useToken()

  const handleMenuClick: MenuProps['onClick'] = ({ key }) => {
    if (key === 'logout') {
      logout()
      navigate('/login', { replace: true })
      return
    }

    if (key === 'profile') {
      message.info('个人中心功能正在开发中')
      return
    }

    navigate(key)
  }

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider
        collapsible
        collapsed={collapsed}
        theme="light"
        style={{ boxShadow: '2px 0 8px rgba(0,0,0,0.1)' }}
        onCollapse={setCollapsed}
      >
        <div
          style={{
            height: 64,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            borderBottom: '1px solid #f0f0f0',
          }}
        >
          <h1
            style={{
              margin: 0,
              fontSize: collapsed ? 14 : 18,
              fontWeight: 'bold',
              color: '#1890ff',
            }}
          >
            {collapsed ? 'JIA' : 'Jamin IA'}
          </h1>
        </div>

        <Menu
          mode="inline"
          selectedKeys={[location.pathname]}
          items={menuItems}
          onClick={handleMenuClick}
          style={{ borderRight: 0 }}
        />
      </Sider>

      <Layout>
        <Header
          style={{
            padding: '0 24px',
            background: colorBgContainer,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
            <Button
              type="text"
              icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
              onClick={() => setCollapsed((current) => !current)}
            />
            <div style={{ fontSize: 16, fontWeight: 500 }}>工业智能监控与诊断平台</div>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
            <Badge count={5} size="small">
              <BellOutlined style={{ fontSize: 18, cursor: 'pointer' }} />
            </Badge>

            <Dropdown menu={{ items: userMenuItems, onClick: handleMenuClick }} placement="bottomRight">
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
                <Avatar icon={<UserOutlined />} />
                <div style={{ display: 'flex', flexDirection: 'column', lineHeight: 1.2 }}>
                  <span>{user?.username || '用户'}</span>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    {user?.role || 'guest'}
                  </Text>
                </div>
              </div>
            </Dropdown>
          </div>
        </Header>

        {children}
      </Layout>
    </Layout>
  )
}

export default MainLayout
