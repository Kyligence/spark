{{/*
Expand the name of the chart.
*/}}
{{- define "yinglong-smart.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "yinglong-smart.fullname" -}}
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
{{- define "yinglong-smart.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "yinglong-smart.labels" -}}
helm.sh/chart: {{ include "yinglong-smart.chart" . }}
{{ include "yinglong-smart.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "yinglong-smart.selectorLabels" -}}
app.kubernetes.io/name: {{ include "yinglong-smart.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app: {{ printf "%s-booter" (include "yinglong-smart.name" .) }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "yinglong-smart.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "yinglong-smart.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
