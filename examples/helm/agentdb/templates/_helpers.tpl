{{/*
Canonical name (release + chart), truncated to 63 chars so
downstream object names stay DNS-1123 compliant.
*/}}
{{- define "agentdb.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
