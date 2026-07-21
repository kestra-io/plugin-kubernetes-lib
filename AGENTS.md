# plugin-kubernetes-lib

## What

- Shared kernel library for the Kestra Kubernetes plugins.
- Holds code common to `plugin-kubernetes` (OSS) and `plugin-ee-kubernetes` (EE) under `io.kestra.plugin.kubernetes.shared`.
- Published as a plain `java-library` jar. `io.fabric8:kubernetes-client` is exposed as `api`, so consumers inherit it transitively.

## Why

- Removes duplicated Kubernetes client, pod, and watcher logic across the OSS and EE repos.
- Gives one place to patch shared behavior instead of two.

## How

### Architecture

Single Gradle module. No plugin tasks or triggers live here, only reusable classes consumed by the two plugin repos.

Source under `io.kestra.plugin.kubernetes.shared`:

- `models` — `Connection`, `OAuthTokenProvider`
- `services` — `ClientService`, `PodService`, `PodLogService`, `InstanceService`, `LoggingOutputStream`
- `watchers` — `AbstractWatch`, `PodWatcher`

### Local rules

- Only code shared by both plugin repos belongs here. Plugin-specific tasks, triggers, and models stay in their own repo.
- Keep the shared surface minimal. If only one repo needs a class, it does not belong here.
- The Kubernetes client version pinned here is the baseline both consumers must align to.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
