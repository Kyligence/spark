{{/*
Expand the name of the chart.
*/}}
{{- define "gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "gateway.fullname" -}}
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
{{- define "gateway.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "gateway.labels" -}}
helm.sh/chart: {{ include "gateway.chart" . }}
{{ include "gateway.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "gateway.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "gateway.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create java tool options
*/}}
{{- define "gateway.toolOptions" -}}
{{- $group := .Values.global.nacos.discovery.group -}}
{{- $publicGroup := .Values.global.nacos.discovery.group -}}
{{- $discoveryServerAddr := default (print "nacos-naming-svc." .Release.Namespace ":8848") .Values.saas.nacos.discovery.serverAddr }}
{{- $configServerAddr := default (print "nacos-config-svc." .Release.Namespace ":8848") .Values.saas.nacos.config.serverAddr }}
{{- if .Values.skyWalking.enabled }}
{{- printf "-javaagent:/skywalking/agent/skywalking-agent.jar -Dspring.profiles.active=nacos -Dsaas.nacos.discovery.group=%s -Dsaas.nacos.discovery.public-group=%s -Dsaas.nacos.discovery.server-addr=%s -Dsaas.nacos.config.server-addr=%s" $group $publicGroup $discoveryServerAddr $configServerAddr }}
{{- else }}
{{- printf "-Dspring.profiles.active=nacos -Dsaas.nacos.discovery.group=%s -Dsaas.nacos.discovery.public-group=%s -Dsaas.nacos.discovery.server-addr=%s -Dsaas.nacos.config.server-addr=%s" $group $publicGroup $discoveryServerAddr $configServerAddr }}
{{- end }}
{{- end }}