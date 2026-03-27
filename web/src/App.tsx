import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { Layout, theme } from 'antd'
import MainLayout from './components/Layout/MainLayout'
import Login from './pages/Login'
import Dashboard from './pages/Dashboard'
import Alerts from './pages/Alerts'
import Diagnosis from './pages/Diagnosis'
import Rules from './pages/Rules'
import Config from './pages/Config'
import { useAuthStore } from './stores/auth'

const { Content } = Layout

function ProtectedApp() {
  const { isAuthenticated } = useAuthStore()
  const {
    token: { colorBgContainer, borderRadiusLG },
  } = theme.useToken()

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />
  }

  return (
    <MainLayout>
      <Content
        style={{
          margin: '24px 16px',
          padding: 24,
          minHeight: 'calc(100vh - 112px)',
          background: colorBgContainer,
          borderRadius: borderRadiusLG,
        }}
      >
        <Routes>
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/alerts" element={<Alerts />} />
          <Route path="/diagnosis" element={<Diagnosis />} />
          <Route path="/rules" element={<Rules />} />
          <Route path="/config" element={<Config />} />
        </Routes>
      </Content>
    </MainLayout>
  )
}

function App() {
  const { isAuthenticated } = useAuthStore()

  return (
    <BrowserRouter>
      <Routes>
        <Route
          path="/login"
          element={isAuthenticated ? <Navigate to="/dashboard" replace /> : <Login />}
        />
        <Route path="/*" element={<ProtectedApp />} />
      </Routes>
    </BrowserRouter>
  )
}

export default App
