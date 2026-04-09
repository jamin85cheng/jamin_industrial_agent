import React, { useMemo, useRef, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Col,
  Input,
  Modal,
  Row,
  Select,
  Space,
  Table,
  Tag,
  Typography,
  message,
} from 'antd'
import type { TableProps } from 'antd'
import { DownloadOutlined, EditOutlined, SettingOutlined, UploadOutlined } from '@ant-design/icons'
import {
  devicesApi,
  extractApiError,
  type DeviceTagImportPreviewResponse,
  type DeviceTagPreviewOverridePayload,
  type DeviceTagPreviewRow,
} from '../../lib/api'

interface TagImportButtonProps {
  disabled?: boolean
  buttonText?: string
  onImported: (preview: DeviceTagImportPreviewResponse) => void
}

interface ImportFieldMeta {
  key: string
  label: string
  description: string
  required?: boolean
}

interface EditablePreviewRowDraft {
  address: string
  data_type: string
  asset_id: string
  point_key: string
}

const FIELD_META: ImportFieldMeta[] = [
  { key: 'name', label: 'Tag Name', description: 'Unique point name shown in the UI.', required: true },
  { key: 'address', label: 'PLC Address', description: 'PLC address such as DB1.DBD0, 40001, or SIM:1.', required: true },
  { key: 'data_type', label: 'Data Type', description: 'float, int, bool, or string.' },
  { key: 'unit', label: 'Unit', description: 'Engineering unit displayed in charts and cards.' },
  { key: 'description', label: 'Description', description: 'Optional notes or compatible metadata.' },
  { key: 'asset_id', label: 'Asset ID', description: 'Asset bound to intelligence patrol.' },
  { key: 'point_key', label: 'Point Key', description: 'Semantic point key used by diagnosis and patrol.' },
  { key: 'deadband', label: 'Deadband', description: 'Minimum change before a new history sample is stored.' },
  { key: 'debounce_ms', label: 'Debounce (ms)', description: 'Minimum stable time before a change is accepted.' },
]

const STATUS_META: Record<string, { color: string; label: string }> = {
  ok: { color: 'green', label: 'Clean' },
  warning: { color: 'gold', label: 'Warning' },
  error: { color: 'red', label: 'Issue' },
}

const ISSUE_LABELS: Record<string, string> = {
  duplicate_address: 'Duplicate address',
  missing_asset_id: 'Missing asset_id',
  missing_point_key: 'Missing point_key',
  unsupported_data_type: 'Unsupported data_type',
  suspicious_type_mismatch: 'Suspicious type mismatch',
}
const CONFIDENCE_COLOR: Record<string, string> = {
  high: 'green',
  medium: 'blue',
  low: 'default',
}
const DATA_TYPE_OPTIONS = ['float', 'int', 'bool', 'string'].map((value) => ({
  label: value,
  value,
}))

const normalizeFieldMapping = (fieldMapping: Record<string, string>) =>
  Object.fromEntries(Object.entries(fieldMapping).filter(([, value]) => value))

const sameFieldMapping = (left: Record<string, string>, right: Record<string, string>) => {
  const leftEntries = Object.entries(normalizeFieldMapping(left)).sort(([a], [b]) => a.localeCompare(b))
  const rightEntries = Object.entries(normalizeFieldMapping(right)).sort(([a], [b]) => a.localeCompare(b))
  return JSON.stringify(leftEntries) === JSON.stringify(rightEntries)
}

const renderFlaggedText = (value: React.ReactNode, flagged: boolean) => (
  <span
    style={{
      background: flagged ? 'rgba(255, 77, 79, 0.12)' : 'transparent',
      border: flagged ? '1px solid rgba(255, 77, 79, 0.24)' : '1px solid transparent',
      borderRadius: 8,
      color: flagged ? '#cf1322' : 'inherit',
      display: 'inline-block',
      maxWidth: '100%',
      padding: '2px 8px',
    }}
  >
    {value}
  </span>
)

const renderPreviewValue = (row: DeviceTagPreviewRow, field: string, value: React.ReactNode, emptyText = 'Not set') => {
  const displayValue = value === undefined || value === null || value === '' ? emptyText : value
  return renderFlaggedText(displayValue, row.flagged_fields.includes(field))
}

const buildSuggestionKey = (rowNumber: number, field: string, value: string) => `${rowNumber}:${field}:${value}`

const buildEditDraft = (row: DeviceTagPreviewRow): EditablePreviewRowDraft => ({
  address: row.tag.address ?? '',
  data_type: row.tag.data_type ?? '',
  asset_id: row.tag.asset_id ?? '',
  point_key: row.tag.point_key ?? '',
})

const buildAutoFixOverrides = (rows: DeviceTagPreviewRow[]): DeviceTagPreviewOverridePayload => {
  const allowedFields = new Set(['asset_id', 'point_key'])
  const overrides: DeviceTagPreviewOverridePayload = {}
  for (const row of rows) {
    for (const suggestion of row.suggestions) {
      if (!allowedFields.has(suggestion.field)) {
        continue
      }
      const currentValue = row.tag[suggestion.field as keyof typeof row.tag]
      if (currentValue !== undefined && currentValue !== null && currentValue !== '') {
        continue
      }
      const rowKey = String(row.row_number)
      overrides[rowKey] = {
        ...(overrides[rowKey] ?? {}),
        [suggestion.field]: suggestion.value,
      }
    }
  }
  return overrides
}

const TagImportButton: React.FC<TagImportButtonProps> = ({
  disabled = false,
  buttonText = 'Import Excel/CSV',
  onImported,
}) => {
  const fileInputRef = useRef<HTMLInputElement | null>(null)
  const [preview, setPreview] = useState<DeviceTagImportPreviewResponse | null>(null)
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [mappingModalOpen, setMappingModalOpen] = useState(false)
  const [fieldMappingDraft, setFieldMappingDraft] = useState<Record<string, string>>({})
  const [acceptedOverrides, setAcceptedOverrides] = useState<DeviceTagPreviewOverridePayload>({})
  const [dismissedSuggestionKeys, setDismissedSuggestionKeys] = useState<string[]>([])
  const [editingRow, setEditingRow] = useState<DeviceTagPreviewRow | null>(null)
  const [editDraft, setEditDraft] = useState<EditablePreviewRowDraft | null>(null)

  const templateMutation = useMutation({
    mutationFn: async (format: 'xlsx' | 'csv') => devicesApi.downloadImportTemplate(format),
    onSuccess: (_, format) => {
      message.success(`Downloaded ${format.toUpperCase()} import template`)
    },
    onError: (error) => {
      message.error(extractApiError(error, 'Failed to download the import template'))
    },
  })

  const previewMutation = useMutation({
    mutationFn: async (file: File) => devicesApi.importTagsPreview(file),
    onSuccess: (result, file) => {
      setSelectedFile(file)
      setPreview(result)
      setFieldMappingDraft(result.field_mapping ?? {})
      setAcceptedOverrides({})
      setDismissedSuggestionKeys([])
      setEditingRow(null)
      setEditDraft(null)
      setMappingModalOpen(true)
      message.success(`Loaded preview for ${result.file_name}`)
    },
    onError: (error) => {
      message.error(extractApiError(error, 'Failed to import point mapping file'))
    },
  })

  const applyMappingMutation = useMutation({
    mutationFn: async ({
      file,
      fieldMapping,
      valueOverrides,
    }: {
      file: File
      fieldMapping: Record<string, string>
      valueOverrides?: DeviceTagPreviewOverridePayload
    }) =>
      devicesApi.importTagsPreview(file, {
        fieldMapping,
        valueOverrides,
      }),
    onSuccess: (result, variables) => {
      setPreview(result)
      setFieldMappingDraft(result.field_mapping ?? {})
      setAcceptedOverrides(variables.valueOverrides ?? {})
      setEditingRow(null)
      setEditDraft(null)
      setMappingModalOpen(false)
      onImported(result)
      if (result.validation_report.has_errors) {
        message.warning('Imported rows applied with validation issues. Please fix the red fields in the form.')
      } else {
        message.success(`Applied ${result.parsed_rows} imported point rows to the form`)
      }
      if (result.warnings.length > 0 || result.skipped_rows > 0) {
        message.warning(`Preview kept ${result.parsed_rows} rows and skipped ${result.skipped_rows} rows`)
      }
    },
    onError: (error) => {
      message.error(extractApiError(error, 'Failed to apply the selected field mapping'))
    },
  })

  const autoFixMutation = useMutation({
    mutationFn: async ({
      file,
      fieldMapping,
      valueOverrides,
    }: {
      file: File
      fieldMapping: Record<string, string>
      valueOverrides: DeviceTagPreviewOverridePayload
      successMessage?: string
      closeEditor?: boolean
    }) =>
      devicesApi.importTagsPreview(file, {
        fieldMapping,
        valueOverrides,
      }),
    onSuccess: (result, variables) => {
      setPreview(result)
      setFieldMappingDraft(result.field_mapping ?? {})
      setAcceptedOverrides(variables.valueOverrides)
      if (variables.closeEditor) {
        setEditingRow(null)
        setEditDraft(null)
      }
      message.success(variables.successMessage ?? 'Applied suggested asset_id and point_key fixes to the preview')
    },
    onError: (error) => {
      message.error(extractApiError(error, 'Failed to apply the suggested fixes'))
    },
  })

  const isBusy =
    disabled ||
    previewMutation.isPending ||
    applyMappingMutation.isPending ||
    autoFixMutation.isPending ||
    templateMutation.isPending

  const duplicateColumns = useMemo(() => {
    const counts = new Map<string, number>()
    for (const value of Object.values(fieldMappingDraft)) {
      if (!value) {
        continue
      }
      counts.set(value, (counts.get(value) ?? 0) + 1)
    }
    return [...counts.entries()].filter(([, count]) => count > 1).map(([column]) => column)
  }, [fieldMappingDraft])

  const missingRequiredFields = useMemo(() => {
    const requiredFields = preview?.required_fields ?? []
    return requiredFields.filter((fieldName) => !fieldMappingDraft[fieldName])
  }, [fieldMappingDraft, preview?.required_fields])

  const issueSummary = useMemo(
    () =>
      Object.entries(preview?.validation_report.issue_counts ?? {}).map(([code, count]) => ({
        code,
        count,
        label: ISSUE_LABELS[code] ?? code,
      })),
    [preview?.validation_report.issue_counts],
  )
  const duplicateClusters = preview?.validation_report.duplicate_clusters ?? []
  const displayPreviewRows = useMemo(
    () =>
      (preview?.preview_rows ?? []).map((row) => ({
        ...row,
        suggestions: row.suggestions.filter(
          (suggestion) =>
            !dismissedSuggestionKeys.includes(buildSuggestionKey(row.row_number, suggestion.field, suggestion.value)),
        ),
      })),
    [dismissedSuggestionKeys, preview?.preview_rows],
  )
  const visibleSuggestionCount = useMemo(
    () => displayPreviewRows.reduce((count, row) => count + row.suggestions.length, 0),
    [displayPreviewRows],
  )
  const autoFixOverrides = useMemo(() => buildAutoFixOverrides(displayPreviewRows), [displayPreviewRows])
  const autoFixCount = useMemo(
    () => Object.values(autoFixOverrides).reduce((count, item) => count + Object.keys(item).length, 0),
    [autoFixOverrides],
  )

  const handleAcceptSuggestion = (row: DeviceTagPreviewRow, field: string, value: string) => {
    if (!selectedFile) {
      message.error('The imported file is no longer available. Please upload it again.')
      return
    }

    const nextOverrides: DeviceTagPreviewOverridePayload = {
      ...acceptedOverrides,
      [String(row.row_number)]: {
        ...(acceptedOverrides[String(row.row_number)] ?? {}),
        [field]: value,
      },
    }

    setDismissedSuggestionKeys((current) =>
      current.filter((item) => item !== buildSuggestionKey(row.row_number, field, value)),
    )
    autoFixMutation.mutate({
      file: selectedFile,
      fieldMapping: normalizeFieldMapping(fieldMappingDraft),
      valueOverrides: nextOverrides,
      successMessage: `Applied suggestion for row ${row.row_number}: ${field} -> ${value}`,
    })
  }

  const handleRejectSuggestion = (row: DeviceTagPreviewRow, field: string, value: string) => {
    const suggestionKey = buildSuggestionKey(row.row_number, field, value)
    setDismissedSuggestionKeys((current) => (current.includes(suggestionKey) ? current : [...current, suggestionKey]))
    message.success(`Dismissed suggestion for row ${row.row_number}: ${field}`)
  }

  const handleRestoreRejectedSuggestions = () => {
    setDismissedSuggestionKeys([])
    message.success('Restored rejected suggestions in the current preview')
  }

  const handleOpenRowEditor = (row: DeviceTagPreviewRow) => {
    setEditingRow(row)
    setEditDraft(buildEditDraft(row))
  }

  const handleCloseRowEditor = () => {
    if (autoFixMutation.isPending) {
      return
    }
    setEditingRow(null)
    setEditDraft(null)
  }

  const handleEditDraftChange = (field: keyof EditablePreviewRowDraft, value: string | undefined) => {
    setEditDraft((current) => (current ? { ...current, [field]: value ?? '' } : current))
  }

  const handleRevalidateEditedRow = () => {
    if (!selectedFile) {
      message.error('The imported file is no longer available. Please upload it again.')
      return
    }
    if (!editingRow || !editDraft) {
      return
    }

    const nextOverrides: DeviceTagPreviewOverridePayload = {
      ...acceptedOverrides,
      [String(editingRow.row_number)]: {
        ...(acceptedOverrides[String(editingRow.row_number)] ?? {}),
        address: editDraft.address,
        data_type: editDraft.data_type,
        asset_id: editDraft.asset_id,
        point_key: editDraft.point_key,
      },
    }

    autoFixMutation.mutate({
      file: selectedFile,
      fieldMapping: normalizeFieldMapping(fieldMappingDraft),
      valueOverrides: nextOverrides,
      successMessage: `Revalidated row ${editingRow.row_number} with manual edits`,
      closeEditor: true,
    })
  }

  const previewColumns = useMemo<TableProps<DeviceTagPreviewRow>['columns']>(
    () => [
      {
        title: 'Row',
        dataIndex: 'row_number',
        width: 72,
      },
      {
        title: 'Status',
        dataIndex: 'status',
        width: 92,
        render: (value: string) => {
          const meta = STATUS_META[value] ?? STATUS_META.warning
          return <Tag color={meta.color}>{meta.label}</Tag>
        },
      },
      {
        title: 'Tag Name',
        key: 'name',
        width: 150,
        render: (_, row) => renderPreviewValue(row, 'name', row.tag.name),
      },
      {
        title: 'Address',
        key: 'address',
        width: 160,
        render: (_, row) => renderPreviewValue(row, 'address', row.tag.address),
      },
      {
        title: 'asset_id',
        key: 'asset_id',
        width: 180,
        render: (_, row) => renderPreviewValue(row, 'asset_id', row.tag.asset_id),
      },
      {
        title: 'point_key',
        key: 'point_key',
        width: 200,
        render: (_, row) => renderPreviewValue(row, 'point_key', row.tag.point_key),
      },
      {
        title: 'data_type',
        key: 'data_type',
        width: 130,
        render: (_, row) => renderPreviewValue(row, 'data_type', row.tag.data_type),
      },
      {
        title: 'Validation Details',
        key: 'issues',
        width: 320,
        render: (_, row) =>
          row.issues.length > 0 ? (
            <Space direction="vertical" size={4}>
              {row.issues.map((issue) => (
                <Typography.Text
                  key={`${row.row_number}-${issue.code}-${issue.field ?? 'row'}`}
                  type={issue.severity === 'error' ? 'danger' : 'warning'}
                >
                  {ISSUE_LABELS[issue.code] ?? issue.code}: {issue.message}
                </Typography.Text>
              ))}
            </Space>
          ) : (
            <Typography.Text type="secondary">No issues detected</Typography.Text>
          ),
      },
      {
        title: 'Repair Suggestions',
        key: 'suggestions',
        width: 320,
        render: (_, row) =>
          row.suggestions.length > 0 ? (
            <Space direction="vertical" size={4}>
              {row.suggestions.map((suggestion, index) => (
                <div key={`${row.row_number}-${suggestion.field}-${index}`}>
                  <Space wrap size={6}>
                    <Tag color={CONFIDENCE_COLOR[suggestion.confidence] ?? 'default'}>
                      {suggestion.confidence}
                    </Tag>
                    <Typography.Text strong>{suggestion.field}</Typography.Text>
                    <Typography.Text code>{suggestion.value}</Typography.Text>
                    <Button
                      size="small"
                      type="link"
                      disabled={isBusy}
                      onClick={() => handleAcceptSuggestion(row, suggestion.field, suggestion.value)}
                    >
                      Accept
                    </Button>
                    <Button
                      size="small"
                      type="link"
                      danger
                      disabled={isBusy}
                      onClick={() => handleRejectSuggestion(row, suggestion.field, suggestion.value)}
                    >
                      Reject
                    </Button>
                  </Space>
                  <Typography.Text type="secondary">{suggestion.reason}</Typography.Text>
                </div>
              ))}
            </Space>
          ) : (
            <Typography.Text type="secondary">No repair suggestion</Typography.Text>
          ),
      },
      {
        title: 'Actions',
        key: 'actions',
        width: 150,
        render: (_, row) => (
          <Button size="small" icon={<EditOutlined />} disabled={isBusy} onClick={() => handleOpenRowEditor(row)}>
            Edit & recheck
          </Button>
        ),
      },
    ],
    [isBusy, handleAcceptSuggestion, handleRejectSuggestion, handleOpenRowEditor],
  )

  const handleSelectFile = () => {
    if (isBusy) {
      return
    }
    fileInputRef.current?.click()
  }

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    event.target.value = ''
    if (!file) {
      return
    }
    previewMutation.mutate(file)
  }

  const handleCloseMappingModal = () => {
    if (applyMappingMutation.isPending || autoFixMutation.isPending) {
      return
    }
    setMappingModalOpen(false)
    setEditingRow(null)
    setEditDraft(null)
  }

  const applyCurrentPreview = (result: DeviceTagImportPreviewResponse) => {
    setMappingModalOpen(false)
    onImported(result)
    if (result.validation_report.has_errors) {
      message.warning('Imported rows applied with validation issues. Please fix the red fields in the form.')
    } else {
      message.success(`Applied ${result.parsed_rows} imported point rows to the form`)
    }
  }

  const handleConfirmMapping = () => {
    if (!preview) {
      return
    }
    if (duplicateColumns.length > 0) {
      message.warning('Each source column can only be mapped to one target field')
      return
    }
    if (missingRequiredFields.length > 0) {
      message.warning(`Please map required fields: ${missingRequiredFields.join(', ')}`)
      return
    }

    const nextMapping = normalizeFieldMapping(fieldMappingDraft)
    if (sameFieldMapping(nextMapping, preview.field_mapping ?? {})) {
      applyCurrentPreview(preview)
      return
    }

    if (!selectedFile) {
      message.error('The imported file is no longer available. Please upload it again.')
      return
    }

    applyMappingMutation.mutate({
      file: selectedFile,
      fieldMapping: nextMapping,
      valueOverrides: acceptedOverrides,
    })
  }

  const handleApplySuggestedFixes = () => {
    if (!preview || !selectedFile) {
      return
    }
    if (autoFixCount === 0) {
      message.info('There are no applicable asset_id/point_key suggestions to auto-apply')
      return
    }
    const nextOverrides: DeviceTagPreviewOverridePayload = { ...acceptedOverrides }
    for (const [rowNumber, values] of Object.entries(autoFixOverrides)) {
      nextOverrides[rowNumber] = {
        ...(nextOverrides[rowNumber] ?? {}),
        ...values,
      }
    }
    autoFixMutation.mutate({
      file: selectedFile,
      fieldMapping: normalizeFieldMapping(fieldMappingDraft),
      valueOverrides: nextOverrides,
      successMessage: 'Applied all visible asset_id and point_key suggestions to the preview',
    })
  }

  const sourceColumnOptions = (preview?.detected_columns ?? []).map((column) => ({
    label: column,
    value: column,
  }))

  const previewAlertType = preview?.validation_report.has_errors
    ? 'error'
    : preview && (preview.warnings.length > 0 || preview.skipped_rows > 0)
      ? 'warning'
      : 'success'

  return (
    <Space direction="vertical" size={10} style={{ width: '100%' }}>
      <input
        ref={fileInputRef}
        type="file"
        accept=".csv,.xlsx"
        style={{ display: 'none' }}
        onChange={handleFileChange}
      />

      <Space wrap>
        <Button
          icon={<UploadOutlined />}
          onClick={handleSelectFile}
          loading={previewMutation.isPending}
          disabled={isBusy}
        >
          {buttonText}
        </Button>
        <Button
          icon={<DownloadOutlined />}
          onClick={() => templateMutation.mutate('xlsx')}
          loading={templateMutation.isPending}
          disabled={isBusy}
        >
          Download XLSX Template
        </Button>
        <Button type="link" onClick={() => templateMutation.mutate('csv')} disabled={isBusy}>
          CSV Template
        </Button>
        <Typography.Text type="secondary">
          Upload a CSV or XLSX file, confirm the column mapping, and review the validation report before applying it.
        </Typography.Text>
      </Space>

      {preview ? (
        <Alert
          type={previewAlertType}
          showIcon
          message={`Preview ready: ${preview.parsed_rows}/${preview.total_rows} rows from ${preview.file_name}`}
          description={
            <Space direction="vertical" size={2}>
              <Typography.Text type="secondary">
                Validation: {preview.validation_report.error_count} issue(s), {preview.validation_report.clean_rows} clean row(s)
              </Typography.Text>
              <Typography.Text type="secondary">
                Repair suggestions: {visibleSuggestionCount}
              </Typography.Text>
              <Typography.Text type="secondary">
                Auto-fixable fields: {autoFixCount}
              </Typography.Text>
              {dismissedSuggestionKeys.length > 0 ? (
                <Typography.Text type="secondary">
                  Rejected suggestions hidden: {dismissedSuggestionKeys.length}
                </Typography.Text>
              ) : null}
              <Typography.Text type="secondary">
                Unmatched columns: {preview.unmatched_columns.join(', ') || 'none'}
              </Typography.Text>
              {preview.warnings.slice(0, 3).map((warning) => (
                <Typography.Text key={warning} type="secondary">
                  {warning}
                </Typography.Text>
              ))}
              {preview.warnings.length > 3 ? (
                <Typography.Text type="secondary">
                  ...and {preview.warnings.length - 3} more warnings.
                </Typography.Text>
              ) : null}
            </Space>
          }
          action={
            <Button size="small" icon={<SettingOutlined />} onClick={() => setMappingModalOpen(true)}>
              Review Mapping
            </Button>
          }
        />
      ) : null}

      <Modal
        title={preview ? `Confirm import mapping: ${preview.file_name}` : 'Confirm import mapping'}
        open={mappingModalOpen}
        onCancel={handleCloseMappingModal}
        onOk={handleConfirmMapping}
        okText="Apply to form"
        confirmLoading={applyMappingMutation.isPending}
        width={1100}
        destroyOnClose={false}
      >
        {preview ? (
          <Space direction="vertical" size={16} style={{ width: '100%' }}>
            <Alert
              type="info"
              showIcon
              message={`Detected ${preview.detected_columns.length} source columns and ${preview.parsed_rows} importable rows`}
              description="Adjust the field mapping, then review the validation report below. Red fields need follow-up before or after applying the preview."
            />

            {duplicateColumns.length > 0 ? (
              <Alert
                type="warning"
                showIcon
                message="One or more source columns are mapped more than once"
                description={`Please resolve duplicates: ${duplicateColumns.join(', ')}`}
              />
            ) : null}

            {missingRequiredFields.length > 0 ? (
              <Alert
                type="warning"
                showIcon
                message="Required fields are not fully mapped"
                description={`Required fields: ${missingRequiredFields.join(', ')}`}
              />
            ) : null}

            <div
              style={{
                display: 'grid',
                gap: 12,
                gridTemplateColumns: 'minmax(180px, 220px) minmax(220px, 1fr) minmax(280px, 1.4fr)',
              }}
            >
              <Typography.Text strong>Target Field</Typography.Text>
              <Typography.Text strong>Source Column</Typography.Text>
              <Typography.Text strong>Description</Typography.Text>

              {FIELD_META.map((field) => (
                <React.Fragment key={field.key}>
                  <Space>
                    <Typography.Text strong>{field.label}</Typography.Text>
                    {field.required ? <Tag color="red">Required</Tag> : <Tag>Optional</Tag>}
                  </Space>
                  <Select
                    allowClear
                    placeholder="Not mapped"
                    value={fieldMappingDraft[field.key]}
                    options={sourceColumnOptions}
                    onChange={(value) =>
                      setFieldMappingDraft((current) => ({
                        ...current,
                        [field.key]: value ?? '',
                      }))
                    }
                  />
                  <Typography.Text type="secondary">{field.description}</Typography.Text>
                </React.Fragment>
              ))}
            </div>

            <Alert
              type={preview.validation_report.has_errors ? 'error' : 'success'}
              showIcon
              message="Import validation report"
              description={
                <Space direction="vertical" size={10} style={{ width: '100%' }}>
                  <Row gutter={[12, 12]}>
                    <Col xs={24} md={6}>
                      <div style={{ border: '1px solid #f0f0f0', borderRadius: 12, padding: 12 }}>
                        <Typography.Text type="secondary">Rows checked</Typography.Text>
                        <Typography.Title level={4} style={{ margin: '6px 0 0' }}>
                          {preview.validation_report.total_rows}
                        </Typography.Title>
                      </div>
                    </Col>
                    <Col xs={24} md={6}>
                      <div style={{ border: '1px solid rgba(255, 77, 79, 0.24)', borderRadius: 12, padding: 12 }}>
                        <Typography.Text type="secondary">Rows with issues</Typography.Text>
                        <Typography.Title level={4} style={{ color: '#cf1322', margin: '6px 0 0' }}>
                          {preview.validation_report.rows_with_errors}
                        </Typography.Title>
                      </div>
                    </Col>
                    <Col xs={24} md={6}>
                      <div style={{ border: '1px solid rgba(82, 196, 26, 0.28)', borderRadius: 12, padding: 12 }}>
                        <Typography.Text type="secondary">Clean rows</Typography.Text>
                        <Typography.Title level={4} style={{ color: '#389e0d', margin: '6px 0 0' }}>
                          {preview.validation_report.clean_rows}
                        </Typography.Title>
                      </div>
                    </Col>
                    <Col xs={24} md={6}>
                      <div style={{ border: '1px solid #f0f0f0', borderRadius: 12, padding: 12 }}>
                        <Typography.Text type="secondary">Suggestions</Typography.Text>
                        <Typography.Title level={4} style={{ margin: '6px 0 0' }}>
                          {visibleSuggestionCount}
                        </Typography.Title>
                      </div>
                    </Col>
                  </Row>

                  {issueSummary.length > 0 ? (
                    <Space wrap>
                      {issueSummary.map((item) => (
                        <Tag key={item.code} color="red">
                          {item.label}: {item.count}
                        </Tag>
                      ))}
                    </Space>
                  ) : (
                    <Typography.Text type="secondary">No validation issues detected.</Typography.Text>
                  )}
                </Space>
              }
            />

            <Space wrap>
              <Button
                type="primary"
                ghost
                disabled={autoFixCount === 0}
                loading={autoFixMutation.isPending}
                onClick={handleApplySuggestedFixes}
              >
                Apply Suggested asset_id / point_key
              </Button>
              <Typography.Text type="secondary">
                This rewrites the preview with recommended asset and semantic keys, then reruns validation before import.
              </Typography.Text>
              <Typography.Text type="secondary">
                You can also edit any row manually and rerun validation before applying the import.
              </Typography.Text>
              {dismissedSuggestionKeys.length > 0 ? (
                <Button type="link" onClick={handleRestoreRejectedSuggestions}>
                  Restore rejected suggestions
                </Button>
              ) : null}
            </Space>

            {duplicateClusters.length > 0 ? (
              <Alert
                type="warning"
                showIcon
                message="Duplicate address clusters"
                description={
                  <Space direction="vertical" size={8} style={{ width: '100%' }}>
                    {duplicateClusters.map((cluster) => (
                      <div
                        key={cluster.cluster_key}
                        style={{
                          background: 'rgba(255, 247, 230, 0.65)',
                          border: '1px solid rgba(250, 173, 20, 0.28)',
                          borderRadius: 12,
                          padding: 12,
                        }}
                      >
                        <Space wrap size={8}>
                          <Tag color="orange">{cluster.label}</Tag>
                          <Typography.Text strong>Rows: {cluster.row_numbers.join(', ')}</Typography.Text>
                          <Typography.Text type="secondary">
                            Addresses: {cluster.addresses.join(', ')}
                          </Typography.Text>
                        </Space>
                        <Typography.Paragraph style={{ margin: '8px 0 0' }} type="secondary">
                          {cluster.suggestion}
                        </Typography.Paragraph>
                      </div>
                    ))}
                  </Space>
                }
              />
            ) : null}

            <Table
              rowKey="row_number"
              size="small"
              columns={previewColumns}
              dataSource={displayPreviewRows}
              pagination={{ pageSize: 6 }}
              scroll={{ x: 1180 }}
            />

            {preview.unmatched_columns.length > 0 ? (
              <Space wrap>
                <Typography.Text type="secondary">Unmatched columns:</Typography.Text>
                {preview.unmatched_columns.map((column) => (
                  <Tag key={column}>{column}</Tag>
                ))}
              </Space>
            ) : null}

            {preview.warnings.length > 0 ? (
              <Alert
                type="warning"
                showIcon
                message={`Parser warnings (${preview.warnings.length})`}
                description={
                  <Space direction="vertical" size={2}>
                    {preview.warnings.slice(0, 6).map((warning) => (
                      <Typography.Text key={warning} type="secondary">
                        {warning}
                      </Typography.Text>
                    ))}
                    {preview.warnings.length > 6 ? (
                      <Typography.Text type="secondary">
                        ...and {preview.warnings.length - 6} more warnings.
                      </Typography.Text>
                    ) : null}
                  </Space>
                }
              />
            ) : null}
          </Space>
        ) : null}
      </Modal>

      <Modal
        title={editingRow ? `Edit preview row ${editingRow.row_number}` : 'Edit preview row'}
        open={Boolean(editingRow && editDraft)}
        onCancel={handleCloseRowEditor}
        onOk={handleRevalidateEditedRow}
        okText="Revalidate row"
        confirmLoading={autoFixMutation.isPending}
        destroyOnClose={false}
      >
        {editingRow && editDraft ? (
          <Space direction="vertical" size={16} style={{ width: '100%' }}>
            <Alert
              type={editingRow.status === 'error' ? 'error' : 'info'}
              showIcon
              message={`Row ${editingRow.row_number}: ${editingRow.tag.name || 'Unnamed tag'}`}
              description="These edits only update the preview. Revalidate the row here first, then apply the whole preview to the form when it looks right."
            />

            <div
              style={{
                display: 'grid',
                gap: 12,
                gridTemplateColumns: 'minmax(140px, 180px) minmax(260px, 1fr)',
              }}
            >
              <Typography.Text strong>PLC Address</Typography.Text>
              <Input
                allowClear
                placeholder="DB1.DBD0 / 40001 / SIM:1"
                value={editDraft.address}
                onChange={(event) => handleEditDraftChange('address', event.target.value)}
              />

              <Typography.Text strong>Data Type</Typography.Text>
              <Select
                allowClear
                showSearch
                placeholder="Select a normalized data type"
                value={editDraft.data_type || undefined}
                options={DATA_TYPE_OPTIONS}
                onChange={(value) => handleEditDraftChange('data_type', value)}
              />

              <Typography.Text strong>Asset ID</Typography.Text>
              <Input
                allowClear
                placeholder="dust_collector_01"
                value={editDraft.asset_id}
                onChange={(event) => handleEditDraftChange('asset_id', event.target.value)}
              />

              <Typography.Text strong>Point Key</Typography.Text>
              <Input
                allowClear
                placeholder="outlet_dust_concentration"
                value={editDraft.point_key}
                onChange={(event) => handleEditDraftChange('point_key', event.target.value)}
              />
            </div>

            <div>
              <Typography.Text strong>Current validation issues</Typography.Text>
              <div style={{ marginTop: 8 }}>
                {editingRow.issues.length > 0 ? (
                  <Space direction="vertical" size={4}>
                    {editingRow.issues.map((issue) => (
                      <Typography.Text
                        key={`${editingRow.row_number}-${issue.code}-${issue.field ?? 'row'}`}
                        type={issue.severity === 'error' ? 'danger' : 'warning'}
                      >
                        {ISSUE_LABELS[issue.code] ?? issue.code}: {issue.message}
                      </Typography.Text>
                    ))}
                  </Space>
                ) : (
                  <Typography.Text type="secondary">This row is currently clean.</Typography.Text>
                )}
              </div>
            </div>
          </Space>
        ) : null}
      </Modal>
    </Space>
  )
}

export default TagImportButton
