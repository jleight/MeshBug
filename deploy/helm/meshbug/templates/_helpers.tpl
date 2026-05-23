{{- define "meshbug.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "meshbug.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "meshbug.labels" -}}
app.kubernetes.io/name: {{ include "meshbug.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "meshbug.selectorLabels" -}}
app.kubernetes.io/name: {{ include "meshbug.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "meshbug.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "meshbug.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Database env vars: MESHBUG_DATABASE_URL is always set from postgres.existingSecret.
MESHBUG_INGEST_DATABASE_URL is only set when postgres.ingestExistingSecret is
configured; otherwise the app falls back to MESHBUG_DATABASE_URL.
*/}}
{{- define "meshbug.databaseEnv" -}}
- name: MESHBUG_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.postgres.existingSecret }}
      key: {{ .Values.postgres.existingSecretKey }}
{{- if .Values.postgres.ingestExistingSecret }}
- name: MESHBUG_INGEST_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.postgres.ingestExistingSecret }}
      key: {{ .Values.postgres.ingestExistingSecretKey }}
{{- end }}
{{- end -}}
