import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { Suspense, lazy, useEffect } from 'react'
import { authApi } from './lib/api'
import { useAuthStore } from './stores/auth'

const MainLayout = lazy(() => import('./components/Layout/MainLayout'))
const Login = lazy(() => import('./pages/Login'))
const Dashboard = lazy(() => import('./pages/Dashboard'))
const Alerts = lazy(() => import('./pages/Alerts'))
const Diagnosis = lazy(() => import('./pages/Diagnosis'))
const Intelligence = lazy(() => import('./pages/Intelligence'))
const Rules = lazy(() => import('./pages/Rules'))
const Config = lazy(() => import('./pages/Config'))

const PageFallback = () => (
  <div style={{ minHeight: '40vh', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#6b7280' }}>
    页面加载中...
  </div>
)

function ProtectedApp() {
  const { isAuthenticated, token, user, updateUser } = useAuthStore()

  useEffect(() => {
    if (!isAuthenticated || !token) {
      return
    }

    if (user?.permissions && user.tenant_id !== undefined) {
      return
    }

    let disposed = false

    void authApi
      .me()
      .then((profile) => {
        if (disposed) {
          return
        }
        updateUser({
          ...profile,
          role: profile.roles?.[0] ?? user?.role ?? 'user',
        })
      })
      .catch(() => {
        // The global response interceptor handles invalid sessions.
      })

    return () => {
      disposed = true
    }
  }, [isAuthenticated, token, updateUser, user?.permissions, user?.role, user?.tenant_id])

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
            <Route path="/intelligence" element={<Intelligence />} />
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
