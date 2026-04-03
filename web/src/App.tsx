import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { Suspense, lazy } from 'react'
import { useAuthStore } from './stores/auth'

const MainLayout = lazy(() => import('./components/Layout/MainLayout'))
const Login = lazy(() => import('./pages/Login'))
const Dashboard = lazy(() => import('./pages/Dashboard'))
const Alerts = lazy(() => import('./pages/Alerts'))
const Diagnosis = lazy(() => import('./pages/Diagnosis'))
const Rules = lazy(() => import('./pages/Rules'))
const Config = lazy(() => import('./pages/Config'))

const PageFallback = () => (
  <div style={{ minHeight: '40vh', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#6b7280' }}>
    页面加载中...
  </div>
)

function ProtectedApp() {
  const { isAuthenticated } = useAuthStore()

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />
  }

  return (
    <MainLayout>
      <div
        style={{
          margin: '24px 16px',
          padding: 24,
          minHeight: 'calc(100vh - 112px)',
          background: '#ffffff',
          borderRadius: 16,
        }}
      >
        <Suspense fallback={<PageFallback />}>
          <Routes>
            <Route path="/" element={<Navigate to="/dashboard" replace />} />
            <Route path="/dashboard" element={<Dashboard />} />
            <Route path="/alerts" element={<Alerts />} />
            <Route path="/diagnosis" element={<Diagnosis />} />
            <Route path="/rules" element={<Rules />} />
            <Route path="/config" element={<Config />} />
          </Routes>
        </Suspense>
      </div>
    </MainLayout>
  )
}

function App() {
  const { isAuthenticated } = useAuthStore()

  return (
    <BrowserRouter>
      <Suspense fallback={<PageFallback />}>
        <Routes>
          <Route
            path="/login"
            element={isAuthenticated ? <Navigate to="/dashboard" replace /> : <Login />}
          />
          <Route path="/*" element={<ProtectedApp />} />
        </Routes>
      </Suspense>
    </BrowserRouter>
  )
}

export default App
