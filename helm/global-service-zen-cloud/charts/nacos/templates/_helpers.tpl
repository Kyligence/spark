{{/*
Expand the name of the chart.
*/}}
{{- define "nacos.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "nacos.fullname" -}}
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
{{- define "nacos.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "nacos.labels" -}}
helm.sh/chart: {{ include "nacos.chart" . }}
{{ include "nacos.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "nacos.selectorLabels" -}}
app.kubernetes.io/name: {{ include "nacos.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "nacos.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "nacos.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
nacos-config-0.nacos-config-headless.nacos.svc.cluster.local:8848
*/}}
{{- define "nacos.nacosConfigServers" -}}
{{- $fullName := include "nacos.fullname" . -}}
{{- $nameSpace := .Release.Namespace -}}
{{- $servicePort := .Values.config.service.port -}}
{{- $list := list }}
{{- range $idx := until (.Values.config.replicaCount | int) }}
{{- $list = append $list (print $fullName "-config-" $idx "." $fullName "-config-headless." $nameSpace ".svc.cluster.local:" $servicePort) }}
{{- end }}
{{- join "," $list }}
{{- end }}

{{/*
nacos-naming-0.nacos-headless.nacos.svc.cluster.local:8848
*/}}
{{- define "nacos.nacosServers" -}}
{{- $fullName := include "nacos.fullname" . -}}
{{- $nameSpace := .Release.Namespace -}}
{{- $servicePort := .Values.naming.service.port -}}
{{- $list := list }}
{{- range $idx := until (.Values.naming.replicaCount | int) }}
{{- $list = append $list (print $fullName "-naming-" $idx "." $fullName "-naming-headless." $nameSpace ".svc.cluster.local:" $servicePort) }}
{{- end }}
{{- join "," $list }}
{{- end }}

{{/*
Create java options
*/}}
{{- define "nacos.nacosConfigtJavaOptions" -}}
{{- $group := .Values.global.nacos.discovery.group -}}
{{- $publicGroup := .Values.global.nacos.discovery.group -}}
{{- $discoveryServerAddr := default (print "nacos-naming-svc." .Release.Namespace ":8848") .Values.config.discovery.serverAddr }}
{{- $configServerAddr := (print "nacos-config-svc." .Release.Namespace ":8848") }}
{{- $appName := (print .Values.config.name | default "nacos-config") }}
{{- if .Values.skyWalking.enabled }}
{{- printf "-javaagent:/skywalking/agent/skywalking-agent.jar -Dspring.profiles.active=nacos -Dsaas.nacos.discovery.group=%s -Dsaas.nacos.discovery.public-group=%s -Dsaas.nacos.discovery.server-addr=%s -Dsaas.nacos.config.server-addr=%s -Dspring.application.name=%s"  $group $publicGroup $discoveryServerAddr $configServerAddr $appName }}
{{- else }}
{{- printf "-Dspring.profiles.active=nacos -Dsaas.nacos.discovery.group=%s -Dsaas.nacos.discovery.public-group=%s -Dsaas.nacos.discovery.server-addr=%s -Dsaas.nacos.config.server-addr=%s -Dspring.application.name=%s"  $group $publicGroup $discoveryServerAddr $configServerAddr $appName }}
{{- end }}
{{- end }}