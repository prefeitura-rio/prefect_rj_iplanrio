# rj_iplanrio__taxirio

Lê as coleções do MongoDB do Taxirio e grava em Delta no `brutos_taxirio`.

## Segredos

A pipeline roda no `taxirio-pool`, em um cluster que não usa o wrapper `utils.secrets`.
O `Secret` de cada ambiente vem do projeto `pipelines` do `Infisical`, com o operador do cluster.
Aplique este manifesto uma vez por ambiente. Troque `infisicalAuthRef` pelo `InfisicalAuth` do cluster e `environmentSlug` por `staging` ou `prod`.

```yaml
apiVersion: secrets.infisical.com/v1beta1
kind: InfisicalStaticSecret
metadata:
  name: pipelines-rj-iplanrio--taxirio-prod
  namespace: <namespace dos jobs do taxirio-pool>
spec:
  infisicalAuthRef:
    name: <InfisicalAuth do cluster>
    namespace: <namespace do operador>
  syncOptions:
    refreshInterval: 60s
  sources:
    - projectSlug: pipelines
      environmentSlug: prod
      secretPath: /rj_iplanrio__taxirio
    - projectSlug: pipelines
      environmentSlug: prod
      secretPath: /shared
  targets:
    - name: pipelines-rj-iplanrio--taxirio-prod
      namespace: <namespace dos jobs do taxirio-pool>
      kind: Secret
      creationPolicy: Owner
```

O nome do `Secret` é `pipelines-rj-iplanrio--taxirio-staging` em `staging` e `pipelines-rj-iplanrio--taxirio-prod` em `prod`. O `secretName` do `prefect.yaml` usa o mesmo nome.
