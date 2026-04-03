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

export interface DiagnosisExpertOpinion {
  expert_type: string
  expert_name: string
  confidence: number
  root_cause: string
  evidence: string[]
  suggestions: string[]
  reasoning: string
  model_name?: string | null
  llm_attempted?: boolean
  llm_used?: boolean
  used_fallback?: boolean
  fallback_reason?: string | null
  response_excerpt?: string | null
  duration_ms?: number | null
  timestamp: string
}

export interface DiagnosisActionItem {
  action: string
  priority: string
  estimated_time?: string
  requires_shutdown?: boolean
}

export interface DiagnosisSparePart {
  name: string
  quantity: number
  spec?: string
}

export interface DiagnosisDebugPayload {
  graph_rag: {
    enabled: boolean
    query?: string
    summary?: Record<string, unknown>
  }
  experts: Record<
    string,
    {
      expert_name: string
      model_name?: string | null
      llm_attempted?: boolean
      llm_used?: boolean
      used_fallback?: boolean
      fallback_reason?: string | null
      response_excerpt?: string | null
    }
  >
  coordinator: {
    model_name?: string | null
    llm_attempted?: boolean
    llm_used?: boolean
    used_fallback?: boolean
    fallback_reason?: string | null
    response_excerpt?: string | null
  }
  execution_trace?: DiagnosisTaskTraceEvent[]
}

export interface DiagnosisTaskTraceEvent {
  stage: string
  message: string
  timestamp: string
  progress?: number
  round?: number
  duration_ms?: number
  agent_id?: string
  agent_key?: string
  agent_name?: string
  used_fallback?: boolean
  diagnosis_id?: string
}

export interface AgentRuntimeProfile {
  expert_name: string
  description?: string
  capabilities?: string[]
  route_key?: string
  model_name?: string | null
  llm_enabled?: boolean
  endpoint?: string | null
  temperature?: number | null
  max_tokens?: number | null
  timeout_seconds?: number | null
  prompt_summary?: string | null
  system_prompt?: string | null
  output_contract?: Record<string, unknown>
}

export interface AgentCatalogItem {
  id: string
  name: string
  type: string
  capabilities: string[]
  description: string
  runtime?: AgentRuntimeProfile
}

export interface DiagnosisResultV2 {
  diagnosis_id: string
  symptoms: string
  final_conclusion: string
  confidence: number
  consensus_level: number
  expert_opinions: DiagnosisExpertOpinion[]
  dissenting_views: DiagnosisExpertOpinion[]
  recommended_actions: DiagnosisActionItem[]
  spare_parts: DiagnosisSparePart[]
  related_cases: string[]
  simulation_scenarios: Array<Record<string, unknown>>
  agent_model_map: Record<string, Record<string, unknown>>
  fallback_summary: Record<string, unknown>
  coordinator_metadata: Record<string, unknown>
  generated_at: string
  debug?: DiagnosisDebugPayload
}

export interface DiagnosisAnalyzeResponseV2 {
  diagnosis_id: string
  status: 'completed' | 'processing'
  message: string
  result?: DiagnosisResultV2
  task_id?: string
}

export interface DiagnosisTaskResponseV2 {
  task_id: string
  task_type?: string
  status: string
  progress: Record<string, unknown>
  runtime?: {
    storage?: string
    target?: string
    persistent?: boolean
    auto_resume?: boolean
    recoverable_state?: boolean
    timeout_seconds?: number
  }
  recovery?: {
    restored_from_persistence?: boolean
    interrupted_by_restart?: boolean
    resume_supported?: boolean
  }
  workflow?: {
    status?: string
    current_stage?: string
    current_round?: number
    round_summaries?: Array<Record<string, unknown>>
    degraded_mode?: boolean
  }
  metadata?: {
    diagnosis_mode?: string
    execution_trace?: DiagnosisTaskTraceEvent[]
    final_result_type?: string
    agent_model_map?: Record<string, Record<string, unknown>>
    coordinator_metadata?: Record<string, unknown>
    fallback_summary?: Record<string, unknown>
    debug?: DiagnosisDebugPayload
    [key: string]: unknown
  }
  result?: Record<string, unknown>
  error?: string | null
  duration_seconds?: number
}

export interface DiagnosisReportExportResponse {
  task_id: string
  format: 'html' | 'pdf' | 'markdown' | 'json'
  path: string
  filename: string
  report_id: string
  generated_at: string
}

export interface DiagnosisTaskListResponseV2 {
  total: number
  tasks: DiagnosisTaskResponseV2[]
}

export type DiagnosisTaskStreamEventType = 'snapshot' | 'heartbeat' | 'complete' | 'error'

export interface DiagnosisTaskStreamHandlers {
  onSnapshot?: (task: DiagnosisTaskResponseV2) => void
  onHeartbeat?: (payload: Record<string, unknown>) => void
  onComplete?: (payload: Record<string, unknown>) => void
  onError?: (error: Error) => void
}

export interface DiagnosisExpertsResponseV2 {
  experts: AgentCatalogItem[]
  coordinator?: AgentCatalogItem | null
}

export interface DiagnosisHistoryResponseV2 {
  total: number
  history: DiagnosisResultV2[]
}

export interface ModelProbeItem {
  route_key: string
  endpoint?: string | null
  model_name?: string | null
  success: boolean
  latency_ms?: number | null
  response_excerpt?: string | null
  error?: string | null
}

export interface ModelProbeResponse {
  success: boolean
  tested_at: string
  probes: ModelProbeItem[]
}

export interface DiagnosisRuntimeDebugResponse {
  process: {
    pid: number
    cwd: string
    python_executable: string
    module_file: string
    sys_path_head: string[]
  }
  config: {
    resolved_path: string
    exists: boolean
    llm_provider?: string
    routing_enabled_in_config?: boolean
    configured_route_keys?: string[]
    endpoint_keys?: string[]
    task_tracking_backend?: string
    postgres_enabled?: boolean
    postgres_database?: string
    postgres_schema?: string
    metadata_database_backend?: string
  }
  runtime: {
    engine_enable_model_routing?: boolean
    router_enabled?: boolean
    task_tracker?: {
      storage?: string
      persistence_path?: string
      active_tasks?: number
      running_tasks?: number
    }
    metadata_database?: {
      backend?: string
      target?: string
    }
    [key: string]: unknown
  }
}

const DEFAULT_API_TIMEOUT_MS = 15_000
const DIAGNOSIS_ANALYZE_TIMEOUT_MS = 12 * 60 * 1000
const DIAGNOSIS_TASK_TIMEOUT_MS = 30_000
const MODEL_PROBE_TIMEOUT_MS = 3 * 60 * 1000

const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || '/api',
  timeout: DEFAULT_API_TIMEOUT_MS,
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

const resolveApiUrl = (path: string) => {
  const baseUrl = api.defaults.baseURL || '/api'
  const normalizedBase = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl
  const normalizedPath = path.startsWith('/') ? path : `/${path}`
  if (/^https?:\/\//i.test(normalizedBase)) {
    return `${normalizedBase}${normalizedPath}`
  }
  return new URL(`${normalizedBase}${normalizedPath}`, window.location.origin).toString()
}

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

export const diagnosisV2Api = {
  async analyze(payload: {
    symptoms: string
    device_id?: string
    sensor_data?: Record<string, number>
    use_multi_agent?: boolean
    use_graph_rag?: boolean
    use_camel?: boolean
    debug?: boolean
    priority?: 'critical' | 'high' | 'normal' | 'low'
  }) {
    const { data } = await api.post<DiagnosisAnalyzeResponseV2>('/v2/diagnosis/analyze', payload, {
      timeout: DIAGNOSIS_ANALYZE_TIMEOUT_MS,
    })
    return data
  },
  async getTask(taskId: string) {
    const { data } = await api.get<DiagnosisTaskResponseV2>(`/v2/diagnosis/task/${taskId}`, {
      timeout: DIAGNOSIS_TASK_TIMEOUT_MS,
    })
    return data
  },
  async listTasks(limit = 10, status?: string) {
    const suffix = status ? `&status=${encodeURIComponent(status)}` : ''
    const { data } = await api.get<DiagnosisTaskListResponseV2>(`/v2/diagnosis/tasks?limit=${limit}${suffix}`, {
      timeout: DIAGNOSIS_TASK_TIMEOUT_MS,
    })
    return data
  },
  async listTasksByMode(limit = 10, diagnosisMode?: 'multi_agent' | 'camel', status?: string) {
    const params = new URLSearchParams({ limit: String(limit) })
    if (diagnosisMode) params.set('diagnosis_mode', diagnosisMode)
    if (status) params.set('status', status)
    const { data } = await api.get<DiagnosisTaskListResponseV2>(`/v2/diagnosis/tasks?${params.toString()}`, {
      timeout: DIAGNOSIS_TASK_TIMEOUT_MS,
    })
    return data
  },
  async getExperts() {
    const { data } = await api.get<DiagnosisExpertsResponseV2>('/v2/diagnosis/experts')
    return data
  },
  async getHistory(limit = 5) {
    const { data } = await api.get<DiagnosisHistoryResponseV2>(`/v2/diagnosis/history?limit=${limit}`)
    return data
  },
  async probeModels() {
    const { data } = await api.post<ModelProbeResponse>('/v2/diagnosis/model-probe', undefined, {
      timeout: MODEL_PROBE_TIMEOUT_MS,
    })
    return data
  },
  async getRuntimeDebug() {
    const { data } = await api.get<DiagnosisRuntimeDebugResponse>('/v2/diagnosis/runtime-debug', {
      timeout: DIAGNOSIS_TASK_TIMEOUT_MS,
    })
    return data
  },
  async exportReport(taskId: string, format: DiagnosisReportExportResponse['format']) {
    const { data } = await api.get<DiagnosisReportExportResponse>(`/v2/diagnosis/task/${taskId}/report?format=${format}`, {
      timeout: DIAGNOSIS_TASK_TIMEOUT_MS,
    })
    return data
  },
  async analyzeAlert(
    alertId: string,
    payload?: {
      use_graph_rag?: boolean
      use_camel?: boolean
      debug?: boolean
      priority?: 'critical' | 'high' | 'normal' | 'low'
      sensor_data?: Record<string, number>
      symptoms_override?: string
    },
  ) {
    const { data } = await api.post<DiagnosisAnalyzeResponseV2>(`/v2/diagnosis/alerts/${alertId}/analyze`, payload ?? {}, {
      timeout: DIAGNOSIS_ANALYZE_TIMEOUT_MS,
    })
    return data
  },
  streamTask(taskId: string, handlers: DiagnosisTaskStreamHandlers) {
    const controller = new AbortController()
    const token = useAuthStore.getState().token
    const url = resolveApiUrl(`/v2/diagnosis/task/${taskId}/events`)

    const run = async () => {
      try {
        const response = await fetch(url, {
          method: 'GET',
          headers: {
            Accept: 'text/event-stream',
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
          },
          signal: controller.signal,
        })

        if (!response.ok) {
          throw new Error(`Task stream request failed with status ${response.status}`)
        }
        if (!response.body) {
          throw new Error('Task stream response body is empty')
        }

        const reader = response.body.getReader()
        const decoder = new TextDecoder()
        let buffer = ''
        let currentEvent: DiagnosisTaskStreamEventType = 'snapshot'
        let currentData = ''

        const flushEvent = () => {
          if (!currentData.trim()) {
            currentEvent = 'snapshot'
            currentData = ''
            return
          }

          try {
            const payload = JSON.parse(currentData) as Record<string, unknown>
            if (currentEvent === 'snapshot') {
              handlers.onSnapshot?.(payload as unknown as DiagnosisTaskResponseV2)
            } else if (currentEvent === 'heartbeat') {
              handlers.onHeartbeat?.(payload)
            } else if (currentEvent === 'complete') {
              handlers.onComplete?.(payload)
            } else {
              const message =
                typeof payload.message === 'string'
                  ? payload.message
                  : typeof payload.detail === 'string'
                    ? payload.detail
                    : 'Task stream reported an error'
              handlers.onError?.(new Error(message))
            }
          } catch (error) {
            handlers.onError?.(error instanceof Error ? error : new Error('Failed to parse task stream payload'))
          }

          currentEvent = 'snapshot'
          currentData = ''
        }

        while (true) {
          const { done, value } = await reader.read()
          if (done) {
            flushEvent()
            break
          }

          buffer += decoder.decode(value, { stream: true })
          const chunks = buffer.split('\n')
          buffer = chunks.pop() || ''

          for (const rawLine of chunks) {
            const line = rawLine.replace(/\r$/, '')
            if (!line) {
              flushEvent()
              continue
            }
            if (line.startsWith(':')) {
              continue
            }
            if (line.startsWith('event:')) {
              currentEvent = line.slice(6).trim() as DiagnosisTaskStreamEventType
              continue
            }
            if (line.startsWith('data:')) {
              currentData += line.slice(5).trim()
            }
          }
        }
      } catch (error) {
        if (controller.signal.aborted) {
          return
        }
        handlers.onError?.(error instanceof Error ? error : new Error('Task stream failed'))
      }
    }

    void run()

    return {
      close() {
        controller.abort()
      },
    }
  },
}
