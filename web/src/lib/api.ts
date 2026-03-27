import axios from 'axios'
import { useAuthStore } from '../stores/auth'

export interface ApiUser {
  user_id: string
  username: string
  role: string
  roles: string[]
  tenant_id?: string | null
  permissions?: string[]
  email?: string
}

export interface LoginPayload {
  username: string
  password: string
}

interface LoginResponse {
  access_token: string
  token_type: string
  expires_in: number
  user: {
    user_id: string
    username: string
    roles: string[]
  }
}

export interface AlertRecord {
  id: string
  rule_id?: string | null
  rule_name?: string | null
  severity: 'critical' | 'warning' | 'info'
  message: string
  device_id?: string | null
  tag?: string | null
  value?: number | null
  threshold?: number | null
  status: 'active' | 'acknowledged' | 'resolved'
  created_at: string
  acknowledged_by?: string | null
  acknowledged_at?: string | null
  resolved_at?: string | null
}

export interface AlertStats {
  total_alerts: number
  active_alerts: number
  critical_alerts: number
  warning_alerts: number
  acknowledged_today: number
}

export interface AlertRule {
  rule_id: string
  name: string
  enabled: boolean
  condition: {
    type: string
    tag?: string
    operator?: string
    value?: number
    expression?: string
  }
  severity: 'critical' | 'warning' | 'info'
  message: string
  suppression_window_minutes: number
  created_at?: string | null
}

export interface DeviceRecord {
  id: string
  name: string
  type: string
  host: string
  port: number
  status: 'online' | 'offline' | 'error'
  last_seen?: string | null
  tag_count: number
  created_at: string
  updated_at: string
  tenant_id?: string | null
}

export interface DiagnoseResponse {
  diagnosis_id: string
  root_cause: string
  confidence: number
  suggestions: string[]
  spare_parts: Array<Record<string, unknown>>
  references: Array<{
    id: string
    title: string
    content: string
    category: string
    tags: string[]
    similarity_score?: number | null
  }>
}

const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || '/api',
  timeout: 15000,
})

api.interceptors.request.use((config) => {
  const token = useAuthStore.getState().token
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error?.response?.status === 401) {
      useAuthStore.getState().logout()
    }
    return Promise.reject(error)
  }
)

const buildUser = (user: LoginResponse['user']): ApiUser => ({
  user_id: user.user_id,
  username: user.username,
  roles: user.roles,
  role: user.roles[0] ?? 'user',
})

export const extractApiError = (error: unknown, fallback = '请求失败，请稍后重试') => {
  if (axios.isAxiosError(error)) {
    const data = error.response?.data
    if (typeof data?.detail === 'string') {
      return data.detail
    }
    if (typeof data?.message === 'string') {
      return data.message
    }
    if (typeof data?.error?.message === 'string') {
      return data.error.message
    }
  }
  if (error instanceof Error && error.message) {
    return error.message
  }
  return fallback
}

export const authApi = {
  async login(payload: LoginPayload) {
    const { data } = await api.post<LoginResponse>('/auth/login', payload)
    return {
      token: data.access_token,
      user: buildUser(data.user),
    }
  },
}

export const alertsApi = {
  async list() {
    const { data } = await api.get<{ total: number; alerts: AlertRecord[] }>('/alerts')
    return data
  },
  async stats() {
    const { data } = await api.get<AlertStats>('/alerts/stats')
    return data
  },
  async acknowledge(alertId: string, comment?: string) {
    const { data } = await api.post(`/alerts/${alertId}/acknowledge`, { comment })
    return data
  },
}

export const rulesApi = {
  async list() {
    const { data } = await api.get<AlertRule[]>('/alerts/rules')
    return data
  },
  async create(payload: AlertRule) {
    const { data } = await api.post<AlertRule>('/alerts/rules', payload)
    return data
  },
  async update(ruleId: string, payload: AlertRule) {
    const { data } = await api.put<AlertRule>(`/alerts/rules/${ruleId}`, payload)
    return data
  },
  async remove(ruleId: string) {
    await api.delete(`/alerts/rules/${ruleId}`)
  },
}

export const devicesApi = {
  async list() {
    const { data } = await api.get<{ total: number; devices: DeviceRecord[] }>('/devices')
    return data
  },
}

export const knowledgeApi = {
  async diagnose(payload: { symptoms: string; device_id?: string }) {
    const { data } = await api.post<DiagnoseResponse>('/knowledge/diagnose', payload)
    return data
  },
}
