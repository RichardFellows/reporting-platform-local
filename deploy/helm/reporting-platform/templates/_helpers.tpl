{{/*
ONE ConfigMap, ONE Secret, and every platform pod names them through these
two helpers -- Airflow's own pods (scheduler/webserver/triggerer/worker,
including the KubernetesExecutor pod template) via `airflow.extraEnvFrom` in
values.yaml, everything else in this chart via `envFrom` built here directly.
See values.yaml's own banner comment and
docs/DECISIONS.md#the-chart-is-the-only-place-settings-are-written.
*/}}

{{- define "reporting-platform.envConfigMap" -}}
{{ .Release.Name }}-platform-env
{{- end -}}

{{/*
The Secret's name. `secrets.existingSecret` wins when set; otherwise it is
the one this chart creates itself (secret-platform.yaml, only when
`secrets.create`).

THE SUBCHART BOUNDARY IS WHY THIS IS TWO PLACES, NOT ONE. `airflow.extraEnvFrom`
is a plain string in values.yaml that the AIRFLOW SUBCHART's own templates
`tpl` at render time, in ITS OWN scope -- `.Release.Name` resolves there
because Release is shared by every chart in a release, but `.Values` in that
scope is the airflow subchart's own values tree, which never contains this
chart's `secrets.existingSecret`. So this helper (used by every template in
*this* chart: inbox, feed-console, job-platform-init, rbac) and
`airflow.extraEnvFrom`'s literal text cannot share one Helm expression -- there
is no template hook that lets a parent chart hand a subchart a value computed
from the parent's own values.

RESOLVED by keeping `airflow.extraEnvFrom`'s DEFAULT exactly the fixed name
below (correct whenever `secrets.existingSecret` is unset, which is every
values file except dev/uat/prod), and having each of THOSE files -- which
already sets `secrets.existingSecret` to a name of its own choosing --
override `airflow.extraEnvFrom` in the SAME file to name the SAME secret
literally. That is one string repeated in one file, not a second value to
invent (a `global.*` mirror would still have to be set by hand in the same
three files, for the same reason, and would be one more name to keep in
step). tests/test_chart.py checks the base file; the per-environment files
are checked by `helm template` actually resolving both to the same string
(see the Done-when transcript).
*/}}
{{- define "reporting-platform.envSecret" -}}
{{- .Values.secrets.existingSecret | default (printf "%s-platform-secrets" .Release.Name) -}}
{{- end -}}

{{/*
An image reference from a `{repo, digest, tag, name, controlled}` dict --
`images.platform` or `images.spark` (plus `name` for the error message and
`controlled`, true in uat/prod). Digest wins when set, so a controlled
environment can also carry a tag (ignored) without failing. A tag alone is
the throwaway-cluster path (`values-local-k8s.yaml`, `kind` loads by tag) and
is REFUSED outside it: PLATFORM_CODE_REF must be a digest, or a re-pushed tag
changes the evidence under an already-published run.
See docs/DECISIONS.md#the-release-image-carries-the-code
*/}}
{{- define "reporting-platform.image" -}}
{{- $img := . -}}
{{- if $img.digest -}}
{{- printf "%s@%s" $img.repo $img.digest -}}
{{- else if $img.tag -}}
{{- if $img.controlled -}}
{{ fail (printf "%s: %q has a tag (%s) but no digest, and %s is a CONTROLLED environment -- only a throwaway cluster may run by tag (docs/DECISIONS.md#the-release-image-carries-the-code). Set .digest." $img.name $img.repo $img.tag $img.environment) }}
{{- else -}}
{{- printf "%s:%s" $img.repo $img.tag -}}
{{- end -}}
{{- else -}}
{{ fail (printf "%s needs .repository and .digest (a throwaway cluster may use .tag instead of .digest)" $img.name) }}
{{- end -}}
{{- end -}}

{{/* true in uat/prod: the CONTROLLED environments (docs/DECISIONS.md#a-change-is-a-deployment-event-not-a-run-event). */}}
{{- define "reporting-platform.controlled" -}}
{{- if or (eq .Values.environment "uat") (eq .Values.environment "prod") -}}true{{- end -}}
{{- end -}}

{{/* The feed console is refused above dev -- it writes into the dbt project,
which is exactly the drift check_project_drift exists to catch
(docs/OPENSHIFT-MAPPING.md, "The feed console is not deployed above dev").
Called from both configmap-platform-env.yaml and feed-console.yaml so the
refusal fires however the chart is rendered, not only when the Deployment
happens to be reached. */}}
{{- define "reporting-platform.refuseFeedConsole" -}}
{{- if and (include "reporting-platform.controlled" .) .Values.feedConsole.enabled -}}
{{ fail (printf "feedConsole.enabled is true in %s, a CONTROLLED environment -- the feed console is dev-only (docs/OPENSHIFT-MAPPING.md)." .Values.environment) }}
{{- end -}}
{{- end -}}

{{- define "reporting-platform.labels" -}}
app.kubernetes.io/part-of: reporting-platform
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}
