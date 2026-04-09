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
  refresh_token: string
  token_type: string
  expires_in: number
  refresh_expires_in: number
  user: {
    user_id: string
    username: string
    roles: string[]
  }
}

interface RefreshSessionPayload {
  refresh_token: string
}

export interface AuthManagedUserRecord {
  user_id: string
  username: string
  roles: string[]
  permissions: string[]
  tenant_id: string
  is_active: boolean
  is_demo: boolean
  created_at?: string | null
  updated_at?: string | null
  last_login_at?: string | null
}

export interface AuthManagedUserListResponse {
  total: number
  users: AuthManagedUserRecord[]
}

export interface AuthCreateUserPayload {
  user_id?: string
  username: string
  password: string
  roles?: string[]
  permissions?: string[]
  tenant_id?: string
  is_active?: boolean
  is_demo?: boolean
}

export interface AuthUpdateUserPayload {
  username?: string
  roles?: string[]
  permissions?: string[]
  tenant_id?: string
  is_active?: boolean
  is_demo?: boolean
}

export interface AuthTenantRecord {
  id: string
  name: string
  status: 'active' | 'suspended' | 'pending' | 'expired'
  created_at?: string | null
  settings: Record<string, unknown>
}

export interface AuthTenantListResponse {
  total: number
  tenants: AuthTenantRecord[]
}

export interface AuthRoleRecord {
  id: string
  name: string
  description: string
  permissions: string[]
  is_system: boolean
  created_at?: string | null
  updated_at?: string | null
}

export interface AuthRoleListResponse {
  total: number
  roles: AuthRoleRecord[]
}

export interface AuthCreateTenantPayload {
  id: string
  name: string
  status?: 'active' | 'suspended' | 'pending' | 'expired'
  settings?: Record<string, unknown>
}

export interface AuthUpdateTenantPayload {
  name?: string
  status?: 'active' | 'suspended' | 'pending' | 'expired'
  settings?: Record<string, unknown>
}

export interface AuthSessionRecord {
  token_id: string
  user_id: string
  username: string
  tenant_id: string
  created_at?: string | null
  expires_at?: string | null
  last_used_at?: string | null
  revoked_at?: string | null
  replaced_by_token_id?: string | null
  user_is_active: boolean
  status: 'active' | 'revoked' | 'expired'
}

export interface AuthSessionListResponse {
  total: number
  sessions: AuthSessionRecord[]
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
  diagnosis_task_id?: string | null
  latest_report_id?: string | null
  latest_report_download_url?: string | null
  last_action_by?: string | null
  last_action_at?: string | null
  resolution_notes?: string | null
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
  rack?: number
  slot?: number
  scan_interval: number
  enabled: boolean
  status: 'online' | 'offline' | 'error'
  last_seen?: string | null
  tag_count: number
  created_at: string
  updated_at: string
  tenant_id?: string | null
}

export interface DeviceTagRecord {
  name: string
  address: string
  data_type: string
  unit?: string | null
  description?: string | null
  asset_id?: string | null
  point_key?: string | null
  deadband?: number | null
  debounce_ms: number
}

export interface DeviceTagImportPreviewResponse {
  file_name: string
  file_type: string
  detected_columns: string[]
  matched_columns: Record<string, string>
  field_mapping: Record<string, string>
  unmatched_columns: string[]
  available_fields: string[]
  required_fields: string[]
  total_rows: number
  parsed_rows: number
  skipped_rows: number
  warnings: string[]
  tags: DeviceTagRecord[]
  preview_rows: DeviceTagPreviewRow[]
  validation_report: DeviceTagValidationSummary
}

export interface DeviceTagValidationIssue {
  code: string
  field?: string | null
  message: string
  severity: 'error' | 'warning' | string
}

export interface DeviceTagRepairSuggestion {
  field: string
  value: string
  confidence: 'low' | 'medium' | 'high' | string
  reason: string
}

export interface DeviceTagPreviewRow {
  row_number: number
  status: 'ok' | 'warning' | 'error' | string
  flagged_fields: string[]
  issues: DeviceTagValidationIssue[]
  suggestions: DeviceTagRepairSuggestion[]
  tag: DeviceTagRecord
}

export interface DeviceTagDuplicateCluster {
  cluster_key: string
  label: string
  addresses: string[]
  row_numbers: number[]
  duplicate_count: number
  suggestion: string
}

export interface DeviceTagValidationSummary {
  total_rows: number
  clean_rows: number
  rows_with_errors: number
  rows_with_warnings: number
  error_count: number
  warning_count: number
  issue_counts: Record<string, number>
  suggestion_count: number
  duplicate_clusters: DeviceTagDuplicateCluster[]
  has_errors: boolean
}

export interface DeviceTagPreviewOverridePayload {
  [rowNumber: string]: Record<string, string | number | null | undefined>
}

export interface DeviceCreatePayload {
  name: string
  type: 's7' | 'modbus' | 'simulated'
  host: string
  port: number
  rack?: number
  slot?: number
  scan_interval?: number
  tags?: DeviceTagRecord[]
}

export interface DeviceUpdatePayload {
  name?: string
  host?: string
  port?: number
  rack?: number
  slot?: number
  scan_interval?: number
  enabled?: boolean
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
    resume_required?: boolean
    resume_count?: number
    last_resumed_at?: string | null
    last_resumed_by?: string | null
  }
  workflow?: {
    status?: string
    current_stage?: string
    current_round?: number
    round_summaries?: Array<Record<string, unknown>>
    degraded_mode?: boolean
  }
  controls?: {
    cancellable?: boolean
    retryable?: boolean
    resumable?: boolean
    cancel_requested_at?: string | null
    cancelled_by?: string | null
    cancellation_reason?: string | null
    retry_count?: number
    retry_of_task_id?: string | null
    resume_count?: number
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
  filename: string
  report_id: string
  generated_at: string
  download_url: string
  media_type: string
  alert_id?: string | null
}

export interface ReportRecord {
  report_id: string
  task_id: string
  diagnosis_id: string
  alert_id?: string | null
  tenant_id: string
  format: 'html' | 'pdf' | 'markdown' | 'json' | string
  filename: string
  media_type: string
  created_at: string
  created_by?: string | null
  file_size_bytes?: number | null
  metadata: Record<string, unknown>
  download_url: string
}

export interface ReportListResponse {
  total: number
  reports: ReportRecord[]
}

export interface SystemConfigPayload {
  basic: {
    system_name: string
    scan_interval: number
    alert_suppression: number
  }
  plc: {
    plc_type: string
    ip_address: string
    port: number
  }
  notifications: {
    feishu_enabled: boolean
    feishu_webhook?: string | null
    email_enabled: boolean
    smtp_server?: string | null
  }
}

export interface SystemConfigResponse {
  config: SystemConfigPayload
  updated_at?: string | null
  updated_by?: string | null
  source: 'defaults' | 'database' | string
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
    diagnosis_execution_backend?: string
    diagnosis_execution_asyncio_workers?: number
    diagnosis_execution_auto_resume_recovered?: boolean
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
      total_queued?: number
    }
    diagnosis_executor?: {
      backend?: string
      requested_backend?: string
      durable?: boolean
      process_isolated?: boolean
      requires_worker?: boolean
      resolution_note?: string | null
      queue_depth?: number
      worker_count?: number
      max_workers?: number
      uses_fastapi_background_tasks?: boolean
    }
    diagnosis_runtime_bootstrap?: {
      bootstrapped_at?: string | null
      auto_resume_enabled?: boolean
      auto_resumed_task_ids?: string[]
      auto_resume_skipped_reason?: string | null
      executor?: {
        backend?: string
        requested_backend?: string
        durable?: boolean
        process_isolated?: boolean
        requires_worker?: boolean
      }
    }
    metadata_database?: {
      backend?: string
      target?: string
    }
    [key: string]: unknown
  }
}

export interface IntelligenceCatalogPlant {
  plant_id: string
  name: string
  description?: string
}

export interface IntelligenceCatalogArea {
  area_id: string
  plant_id: string
  name: string
  description?: string
}

export interface IntelligenceCatalogLine {
  line_id: string
  area_id: string
  name: string
  description?: string
}

export interface IntelligencePointDefinition {
  point_id: string
  display_name: string
  unit: string
  point_type: string
  required: boolean
  low_limit?: number | null
  high_limit?: number | null
  description?: string
}

export interface IntelligenceAssetDefinition {
  asset_id: string
  line_id: string
  area_id: string
  plant_id: string
  scene_type: string
  name: string
  description?: string
  points: IntelligencePointDefinition[]
}

export interface IntelligenceAssetCatalogResponse {
  plants: IntelligenceCatalogPlant[]
  areas: IntelligenceCatalogArea[]
  lines: IntelligenceCatalogLine[]
  assets: IntelligenceAssetDefinition[]
}

export interface IntelligenceRuntimeSummary {
  default_scene: string
  patrol_interval_seconds: number
  assets: number
  latest_run_id?: string | null
  pending_review_count: number
  confirmed_label_count: number
  knowledge_case_count: number
  candidate_count: number
}

export interface IntelligenceSchedulerSummary {
  running: boolean
  interval_seconds?: number | null
  last_run_id?: string | null
  last_error?: string | null
}

export interface IntelligenceRuntimeResponse {
  service: IntelligenceRuntimeSummary
  scheduler: IntelligenceSchedulerSummary
}

export interface IntelligenceSnapshotPoint {
  point_id: string
  display_name: string
  value: string | number | boolean
  unit?: string
  quality: string
  timestamp: string
  history?: number[]
  point_type?: string
}

export interface IntelligenceSnapshot {
  snapshot_id: string
  asset_id: string
  asset_name?: string
  scene_type: string
  collected_at: string
  source: string
  completeness: number
  points: Record<string, IntelligenceSnapshotPoint>
}

export interface IntelligencePatrolFinding {
  code: string
  severity: string
  title: string
  description: string
  affected_points: string[]
  evidence: Record<string, unknown>
}

export interface IntelligencePredictionWindow {
  horizon_minutes: number
  risk_score: number
  summary: string
  fault_probabilities: Record<string, number>
}

export interface IntelligenceKnowledgeCase {
  case_id: string
  asset_id?: string | null
  scene_type: string
  title: string
  summary: string
  content: string
  tags: string[]
  root_cause?: string | null
  recommended_actions: string[]
  source_label_id?: string | null
  source_type: string
  usage_count: number
  created_at?: string | null
  updated_at?: string | null
  score?: number
}

export interface IntelligenceAssetAssessment {
  asset_id: string
  asset_name: string
  scene_type: string
  status: string
  risk_score: number
  risk_level: string
  suspected_faults: string[]
  affected_points: string[]
  operator_actions: string[]
  requires_review: boolean
  summary: string
  findings: IntelligencePatrolFinding[]
  knowledge_hits: IntelligenceKnowledgeCase[]
  knowledge_grounding_ratio: number
  prediction_window: IntelligencePredictionWindow[]
  snapshot_id?: string | null
  review_label_id?: string | null
}

export interface IntelligenceLabelRecord {
  label_id: string
  run_id: string
  asset_id: string
  asset_name?: string
  scene_type: string
  status: string
  anomaly_type?: string | null
  root_cause?: string | null
  created_at?: string | null
  updated_at?: string | null
  review: Record<string, unknown>
  current_assessment?: IntelligenceAssetAssessment
}

export interface IntelligenceReviewResult {
  label: IntelligenceLabelRecord
  knowledge_case?: IntelligenceKnowledgeCase | null
}

export interface IntelligencePatrolRun {
  run_id: string
  task_id?: string
  scene_type: string
  status: string
  risk_level: string
  risk_score: number
  created_at: string
  triggered_by: string
  schedule_type: string
  asset_results: IntelligenceAssetAssessment[]
  review_queue_size: number
  abnormal_asset_count: number
  healthy_asset_count: number
  labels_created?: IntelligenceLabelRecord[]
}

export interface IntelligenceLatestRiskResponse {
  run_id?: string | null
  risk_level?: string | null
  risk_score?: number | null
  asset_results: IntelligenceAssetAssessment[]
}

export interface IntelligenceLearningCandidate {
  candidate_id: string
  candidate_type: string
  name: string
  status: string
  score: number
  rationale: string
  payload: Record<string, unknown>
  created_at?: string | null
  updated_at?: string | null
}

export interface IntelligenceTelemetryPointInput {
  value: string | number | boolean
  unit?: string
  quality?: string
  timestamp?: string
}

export interface IntelligenceDemoSeedResponse {
  profile: 'normal' | 'warning' | 'critical'
  snapshot: IntelligenceSnapshot
  patrol_run?: IntelligencePatrolRun | null
}

const DEFAULT_API_TIMEOUT_MS = 15_000
const DIAGNOSIS_ANALYZE_TIMEOUT_MS = 12 * 60 * 1000
const DIAGNOSIS_TASK_TIMEOUT_MS = 30_000
const MODEL_PROBE_TIMEOUT_MS = 3 * 60 * 1000

const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || '/api',
  timeout: DEFAULT_API_TIMEOUT_MS,
})

const authSessionClient = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || '/api',
  timeout: DEFAULT_API_TIMEOUT_MS,
})

let refreshPromise: Promise<string | null> | null = null

api.interceptors.request.use((config) => {
  const token = useAuthStore.getState().token
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

api.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest = error?.config
    const requestUrl = String(originalRequest?.url ?? '')
    const isAuthSessionRequest =
      requestUrl.includes('/auth/login') ||
      requestUrl.includes('/auth/refresh') ||
      requestUrl.includes('/auth/logout')

    if (error?.response?.status === 401 && originalRequest && !originalRequest.__retry && !isAuthSessionRequest) {
      if (!refreshPromise) {
        refreshPromise = refreshAccessToken().finally(() => {
          refreshPromise = null
        })
      }

      const nextToken = await refreshPromise
      if (nextToken) {
        originalRequest.__retry = true
        originalRequest.headers = {
          ...(originalRequest.headers ?? {}),
          Authorization: `Bearer ${nextToken}`,
        }
        return api(originalRequest)
      }
    }

    if (error?.response?.status === 401) {
      useAuthStore.getState().logout()
    }
    return Promise.reject(error)
  }
)

const refreshAccessToken = async (): Promise<string | null> => {
  const { refreshToken, updateTokens, updateUser, logout } = useAuthStore.getState()
  if (!refreshToken) {
    logout()
    return null
  }

  try {
    const { data } = await authSessionClient.post<LoginResponse>('/auth/refresh', {
      refresh_token: refreshToken,
    } as RefreshSessionPayload)
    updateTokens(
      data.access_token,
      data.refresh_token,
      data.expires_in,
      data.refresh_expires_in,
    )
    updateUser({
      ...buildUser(data.user),
    })
    return data.access_token
  } catch {
    logout()
    return null
  }
}

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

const downloadFileFromApi = async (path: string, filename: string) => {
  const { data } = await api.get<Blob>(path, {
    responseType: 'blob',
    timeout: DIAGNOSIS_TASK_TIMEOUT_MS,
  })
  const blobUrl = window.URL.createObjectURL(data)
  const anchor = document.createElement('a')
  anchor.href = blobUrl
  anchor.download = filename
  document.body.appendChild(anchor)
  anchor.click()
  anchor.remove()
  window.URL.revokeObjectURL(blobUrl)
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
      refreshToken: data.refresh_token,
      expiresIn: data.expires_in,
      refreshExpiresIn: data.refresh_expires_in,
      user: buildUser(data.user),
    }
  },
  async refresh(payload: RefreshSessionPayload) {
    const { data } = await authSessionClient.post<LoginResponse>('/auth/refresh', payload)
    return data
  },
  async logout(refreshToken?: string | null) {
    if (!refreshToken) {
      return { success: true }
    }
    const { data } = await authSessionClient.post<{ success: boolean }>('/auth/logout', {
      refresh_token: refreshToken,
    } as RefreshSessionPayload)
    return data
  },
  async me() {
    const { data } = await api.get<ApiUser>('/auth/me')
    return data
  },
  async listUsers(params?: { tenant_id?: string; include_inactive?: boolean }) {
    const query = new URLSearchParams()
    if (params?.tenant_id) {
      query.set('tenant_id', params.tenant_id)
    }
    if (params?.include_inactive !== undefined) {
      query.set('include_inactive', String(params.include_inactive))
    }
    const suffix = query.toString()
    const { data } = await api.get<AuthManagedUserListResponse>(`/auth/users${suffix ? `?${suffix}` : ''}`)
    return data
  },
  async listRoles() {
    const { data } = await api.get<AuthRoleListResponse>('/auth/roles')
    return data
  },
  async createUser(payload: AuthCreateUserPayload) {
    const { data } = await api.post<AuthManagedUserRecord>('/auth/users', payload)
    return data
  },
  async updateUser(userId: string, payload: AuthUpdateUserPayload) {
    const { data } = await api.patch<AuthManagedUserRecord>(`/auth/users/${userId}`, payload)
    return data
  },
  async listTenants() {
    const { data } = await api.get<AuthTenantListResponse>('/auth/tenants')
    return data
  },
  async listSessions(params?: { tenant_id?: string; user_id?: string; include_revoked?: boolean }) {
    const query = new URLSearchParams()
    if (params?.tenant_id) {
      query.set('tenant_id', params.tenant_id)
    }
    if (params?.user_id) {
      query.set('user_id', params.user_id)
    }
    if (params?.include_revoked !== undefined) {
      query.set('include_revoked', String(params.include_revoked))
    }
    const suffix = query.toString()
    const { data } = await api.get<AuthSessionListResponse>(`/auth/sessions${suffix ? `?${suffix}` : ''}`)
    return data
  },
  async revokeSession(tokenId: string) {
    const { data } = await api.post<AuthSessionRecord>(`/auth/sessions/${tokenId}/revoke`)
    return data
  },
  async createTenant(payload: AuthCreateTenantPayload) {
    const { data } = await api.post<AuthTenantRecord>('/auth/tenants', payload)
    return data
  },
  async updateTenant(tenantId: string, payload: AuthUpdateTenantPayload) {
    const { data } = await api.patch<AuthTenantRecord>(`/auth/tenants/${tenantId}`, payload)
    return data
  },
}

export const systemConfigApi = {
  async get() {
    const { data } = await api.get<SystemConfigResponse>('/system/config')
    return data
  },
  async save(payload: SystemConfigPayload) {
    const { data } = await api.put<SystemConfigResponse>('/system/config', payload)
    return data
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
  async resolve(alertId: string, notes?: string) {
    const { data } = await api.post(`/alerts/${alertId}/resolve`, { notes })
    return data
  },
}

export const reportsApi = {
  async list(params?: { task_id?: string; alert_id?: string; limit?: number }) {
    const query = new URLSearchParams()
    if (params?.task_id) {
      query.set('task_id', params.task_id)
    }
    if (params?.alert_id) {
      query.set('alert_id', params.alert_id)
    }
    if (params?.limit) {
      query.set('limit', String(params.limit))
    }
    const suffix = query.toString()
    const { data } = await api.get<ReportListResponse>(`/reports${suffix ? `?${suffix}` : ''}`)
    return data
  },
  async get(reportId: string) {
    const { data } = await api.get<ReportRecord>(`/reports/${reportId}`)
    return data
  },
  async download(reportId: string, filename?: string) {
    const metadata = filename
      ? null
      : await api.get<ReportRecord>(`/reports/${reportId}`).then((response) => response.data)
    await downloadFileFromApi(`/reports/${reportId}/download`, filename || metadata?.filename || `${reportId}.bin`)
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
  async get(deviceId: string) {
    const { data } = await api.get<DeviceRecord>(`/devices/${deviceId}`)
    return data
  },
  async create(payload: DeviceCreatePayload) {
    const { data } = await api.post<DeviceRecord>('/devices', payload)
    return data
  },
  async update(deviceId: string, payload: DeviceUpdatePayload) {
    const { data } = await api.put<DeviceRecord>(`/devices/${deviceId}`, payload)
    return data
  },
  async listTags(deviceId: string) {
    const { data } = await api.get<DeviceTagRecord[]>(`/devices/${deviceId}/tags`)
    return data
  },
  async replaceTags(deviceId: string, tags: DeviceTagRecord[]) {
    const { data } = await api.put<DeviceTagRecord[]>(`/devices/${deviceId}/tags`, { tags })
    return data
  },
  async downloadImportTemplate(format: 'xlsx' | 'csv' = 'xlsx') {
    const filename = format === 'csv' ? 'device_tag_import_template.csv' : 'device_tag_import_template.xlsx'
    await downloadFileFromApi(`/devices/tags/import-template?format=${format}`, filename)
  },
  async importTagsPreview(
    file: File,
    options?: {
      fieldMapping?: Record<string, string>
      valueOverrides?: DeviceTagPreviewOverridePayload
    },
  ) {
    const formData = new FormData()
    formData.append('file', file)
    if (options?.fieldMapping && Object.keys(options.fieldMapping).length > 0) {
      formData.append('field_mapping', JSON.stringify(options.fieldMapping))
    }
    if (options?.valueOverrides && Object.keys(options.valueOverrides).length > 0) {
      formData.append('value_overrides', JSON.stringify(options.valueOverrides))
    }
    const { data } = await api.post<DeviceTagImportPreviewResponse>('/devices/tags/import-preview', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
      timeout: 60_000,
    })
    return data
  },
  async connect(deviceId: string) {
    const { data } = await api.post<{ status: string; device_id: string }>(`/devices/${deviceId}/connect`)
    return data
  },
  async disconnect(deviceId: string) {
    const { data } = await api.post<{ status: string; device_id: string }>(`/devices/${deviceId}/disconnect`)
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
  async cancelTask(taskId: string, reason?: string) {
    const { data } = await api.post<DiagnosisTaskResponseV2>(`/v2/diagnosis/task/${taskId}/cancel`, {
      reason,
    })
    return data
  },
  async retryTask(taskId: string) {
    const { data } = await api.post<DiagnosisAnalyzeResponseV2>(`/v2/diagnosis/task/${taskId}/retry`)
    return data
  },
  async resumeTask(taskId: string) {
    const { data } = await api.post<DiagnosisAnalyzeResponseV2>(`/v2/diagnosis/task/${taskId}/resume`)
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

export const intelligenceApi = {
  async listAssets() {
    const { data } = await api.get<IntelligenceAssetCatalogResponse>('/intelligence/assets')
    return data
  },
  async getRuntime() {
    const { data } = await api.get<IntelligenceRuntimeResponse>('/intelligence/runtime')
    return data
  },
  async ingestTelemetry(payload: { asset_id: string; source?: string; points: Record<string, IntelligenceTelemetryPointInput> }) {
    const { data } = await api.post<IntelligenceSnapshot>('/intelligence/telemetry/ingest', payload)
    return data
  },
  async seedDemo(payload?: {
    asset_id?: string
    profile?: 'normal' | 'warning' | 'critical'
    run_patrol?: boolean
  }) {
    const { data } = await api.post<IntelligenceDemoSeedResponse>('/intelligence/demo/seed', payload ?? {})
    return data
  },
  async listLatestSnapshots(assetIds?: string[]) {
    const query = new URLSearchParams()
    for (const assetId of assetIds ?? []) {
      query.append('asset_ids', assetId)
    }
    const suffix = query.toString()
    const { data } = await api.get<IntelligenceSnapshot[]>(`/intelligence/snapshots/latest${suffix ? `?${suffix}` : ''}`)
    return data
  },
  async runPatrol(payload?: { asset_ids?: string[]; schedule_type?: 'manual' | 'scheduled' | 'shadow' }) {
    const { data } = await api.post<IntelligencePatrolRun>('/intelligence/patrol/run', payload ?? {})
    return data
  },
  async listPatrolRuns(limit = 20) {
    const { data } = await api.get<IntelligencePatrolRun[]>(`/intelligence/patrol/runs?limit=${limit}`)
    return data
  },
  async getPatrolRun(runId: string) {
    const { data } = await api.get<IntelligencePatrolRun>(`/intelligence/patrol/runs/${runId}`)
    return data
  },
  async getLatestRisk() {
    const { data } = await api.get<IntelligenceLatestRiskResponse>('/intelligence/risk/latest')
    return data
  },
  async listReviewQueue(params?: { status?: string; limit?: number }) {
    const query = new URLSearchParams()
    if (params?.status) query.set('status', params.status)
    if (params?.limit) query.set('limit', String(params.limit))
    const suffix = query.toString()
    const { data } = await api.get<IntelligenceLabelRecord[]>(`/intelligence/review-queue${suffix ? `?${suffix}` : ''}`)
    return data
  },
  async reviewLabel(labelId: string, payload: {
    anomaly_type?: string
    root_cause?: string
    review_notes?: string
    final_action?: string
    false_positive?: boolean
  }) {
    const { data } = await api.post<IntelligenceReviewResult>(`/intelligence/review-queue/${labelId}/review`, payload)
    return data
  },
  async listKnowledgeCases(params?: { scene_type?: string; limit?: number }) {
    const query = new URLSearchParams()
    if (params?.scene_type) query.set('scene_type', params.scene_type)
    if (params?.limit) query.set('limit', String(params.limit))
    const suffix = query.toString()
    const { data } = await api.get<IntelligenceKnowledgeCase[]>(`/intelligence/knowledge/cases${suffix ? `?${suffix}` : ''}`)
    return data
  },
  async generateLearningCandidates(candidateTypes?: string[]) {
    const { data } = await api.post<IntelligenceLearningCandidate[]>('/intelligence/learning/candidates/generate', {
      candidate_types: candidateTypes,
    })
    return data
  },
  async listLearningCandidates(params?: { candidate_type?: string; limit?: number }) {
    const query = new URLSearchParams()
    if (params?.candidate_type) query.set('candidate_type', params.candidate_type)
    if (params?.limit) query.set('limit', String(params.limit))
    const suffix = query.toString()
    const { data } = await api.get<IntelligenceLearningCandidate[]>(`/intelligence/learning/candidates${suffix ? `?${suffix}` : ''}`)
    return data
  },
}
