{{/* Common naming helpers */}}
{{- define "maestro.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "maestro.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s" (include "maestro.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "maestro.labels" -}}
app.kubernetes.io/name: {{ include "maestro.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "maestro.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (printf "%s" (include "maestro.fullname" .)) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "maestro.leaseNamespace" -}}
{{- default .Release.Namespace .Values.flink.leaseNamespace -}}
{{- end -}}

{{/*
Resolve the effective Temporal frontend address. In bundled mode the in-chart
Temporal service is used; otherwise the operator-supplied address.
*/}}
{{- define "maestro.temporalAddress" -}}
{{- if eq .Values.temporal.mode "bundled" -}}
{{- printf "%s-temporal:7233" (include "maestro.fullname" .) -}}
{{- else -}}
{{- required "temporal.address is required when temporal.mode=external" .Values.temporal.address -}}
{{- end -}}
{{- end -}}

{{/*
Common environment shared by control-api and worker.
*/}}
{{- define "maestro.commonEnv" -}}
- name: TEMPORAL_ADDRESS
  value: {{ include "maestro.temporalAddress" . | quote }}
- name: TEMPORAL_NAMESPACE
  value: {{ .Values.temporal.namespace | quote }}
- name: ACTOR_TASK_QUEUE
  value: {{ .Values.taskQueue.actor | quote }}
- name: ACTOR_TASK_QUEUE_SHARDS
  value: {{ .Values.taskQueue.shards | quote }}
- name: ACTIVITY_TASK_QUEUE
  value: {{ .Values.taskQueue.activity | quote }}
{{- if .Values.temporal.tls }}
- name: TEMPORAL_TLS
  value: "true"
{{- end }}
{{- if .Values.temporal.apiKey.secretName }}
- name: TEMPORAL_API_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.temporal.apiKey.secretName }}
      key: {{ .Values.temporal.apiKey.secretKey }}
{{- end }}
{{- end -}}
