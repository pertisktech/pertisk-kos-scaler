{{/*
Expand the name of the chart.
*/}}
{{- define "pertisk-kos-scaler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "pertisk-kos-scaler.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "pertisk-kos-scaler.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "pertisk-kos-scaler.labels" -}}
helm.sh/chart: {{ include "pertisk-kos-scaler.chart" . }}
{{ include "pertisk-kos-scaler.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "pertisk-kos-scaler.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pertisk-kos-scaler.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "pertisk-kos-scaler.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "pertisk-kos-scaler.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Get the secret name containing Pertisk management credentials
*/}}
{{- define "pertisk-kos-scaler.secretName" -}}
{{- if .Values.mgmt.existingSecret }}
{{- .Values.mgmt.existingSecret }}
{{- else }}
{{- include "pertisk-kos-scaler.fullname" . }}-credentials
{{- end }}
{{- end }}
