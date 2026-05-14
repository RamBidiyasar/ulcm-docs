# UCLM Services Deployment Strategies

This document outlines the deployment strategy for each service within the UCLM architecture based on the presence of deployment manifests (`deployment.yaml`), Helm charts (`helm/` directories), and deployment documentation (`DEPLOYMENT.md`).

## Deployment Analysis

| Domain / Category | Service Name | Deployment Assets Found | Deployment Strategy |
| :--- | :--- | :--- | :--- |
| **Bummlebee** | `uclm-dlr-aerospike-cache-loader` | `deployment.yaml`, `helm/` | Single generic deployment. Supports both direct Kubernetes manifests and Helm chart deployments. |
| **Bummlebee** | `uclm-dlr-api-service` | `deployment.yaml` | Single generic Kubernetes deployment via raw manifest. |
| **Bummlebee** | `uclm-dlr-enricher` | `deployment.yaml`, `helm/` | Single generic deployment. Supports both direct Kubernetes manifests and Helm chart deployments. |
| **Bummlebee** | `uclm-orchestrator-service` | `helm/orchestrator/templates/deployment.yaml` | Helm-based deployment exclusively. |
| **Bummlebee** | `uclm-rate-controller-service` | `deployment.yaml` | Single generic Kubernetes deployment via raw manifest. |
| **Bummlebee** | `uclm-validation-governance-service` | `deployment-d2c.yaml`, `deployment-email.yaml`, `deployment-sms-iq.yaml`, `deployment-rcs.yaml`, `deployment-wa.yaml`, etc. | **Channel-wise Deployment Strategy.** The service runs as multiple separate workloads, with a distinct deployment per communication channel (Email, SMS, WhatsApp, RCS, Push, D2C, etc.). |
| **Comms** | `uclm-analytics-reporting-service` | `deployment.yaml` | Single generic Kubernetes deployment via raw manifest. |
| **Comms** | `uclm-campaign-audience-push` | `helm/`, `DEPLOYMENT.md` | Helm-based deployment, supplemented with specific deployment instructions (`DEPLOYMENT.md`). |
| **Comms** | `uclm-campaign-cg-exclusion` | `deployment.yaml` | Single generic Kubernetes deployment via raw manifest. |
| **Comms** | `uclm-campaign-data-file-dowload` | `deployment.yaml` | Single generic Kubernetes deployment via raw manifest. |
| **Comms** | `uclm-campaign-exclusion-scan` | `deployment.yaml` | Single generic Kubernetes deployment via raw manifest. |
| **Comms** | `uclm-campaign-manager-event-enrichment`| `deployment.yaml` | Single generic Kubernetes deployment via raw manifest. |
| **Comms** | `uclm-campaign-manager` | `helm/`, `DEPLOYMENT.md` | Helm-based deployment, supplemented with specific deployment instructions (`DEPLOYMENT.md`). |
| **Comms** | `uclm-campaign-processor` | `deployment.yaml` | Single generic Kubernetes deployment via raw manifest. |
| **Comms** | `uclm-campaign-time-validation` | `deployment.yaml` | Single generic Kubernetes deployment via raw manifest. |
| **Comms** | `uclm-test-campaign` | `deployment.yaml` | Single generic Kubernetes deployment via raw manifest. |
| **User Management**| `uclm-auth-manager` | `deployment.yaml` | Single generic Kubernetes deployment via raw manifest. |
| **User Management**| `uclm-contentmgmt` | `deployment.yaml` | Single generic Kubernetes deployment via raw manifest. |

## Key Takeaways
1. **Predominantly Single Deployments:** Most services utilize a straightforward single generic `deployment.yaml` or a dedicated Helm chart (`helm/`).
2. **Channel-wise Isolation:** `uclm-validation-governance-service` stands out by adopting a heavily decoupled, channel-wise deployment architecture. It splits traffic and governance rules into isolated deployments across various channels (e.g., `email`, `push`, `rcs`, `sms-iq`, `wa`).
3. **Helm Adoption:** Helm is partially adopted in newer or more complex services like `uclm-campaign-manager`, `uclm-campaign-audience-push`, and `uclm-orchestrator-service`.
4. **Documentation:** Certain `comms` components include formal `DEPLOYMENT.md` files indicating more specialized environment setup or configuration instructions.