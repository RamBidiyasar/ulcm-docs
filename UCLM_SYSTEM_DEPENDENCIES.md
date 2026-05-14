# UCLM System Dependencies Overview

# uclm-dlr-aerospike-cache-loader Dependencies

## Databases
- Aerospike
- Aerospike config: 3000} (from application-email-netcore-prod.yml)
- Aerospike config: 3000} (from application-push-prod.yml)
- Aerospike config: 3000} (from application-rcs-prod.yml)
- Aerospike config: 3000} (from application-sms-prod.yml)
- Aerospike config: 3000} (from application-wa-prod.yml)

## Messaging / Brokers
- Kafka
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-email-netcore-dev.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-email-netcore-uat.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-push-dev.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-push-uat.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-rcs-uat.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-sms-dev.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-sms-uat.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-wa-dev.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-wa-uat.yml)
- Kafka Bootstrap Servers: kafka-prod-1:9092,kafka-prod-2:9092,kafka-prod-3:9092} (from application-email-netcore-prod.yml)
- Kafka Bootstrap Servers: kafka-prod-1:9092,kafka-prod-2:9092,kafka-prod-3:9092} (from application-push-prod.yml)
- Kafka Bootstrap Servers: kafka-prod-1:9092,kafka-prod-2:9092,kafka-prod-3:9092} (from application-rcs-prod.yml)
- Kafka Bootstrap Servers: kafka-prod-1:9092,kafka-prod-2:9092,kafka-prod-3:9092} (from application-sms-prod.yml)
- Kafka Bootstrap Servers: kafka-prod-1:9092,kafka-prod-2:9092,kafka-prod-3:9092} (from application-wa-prod.yml)
- Kafka Bootstrap Servers: kafka-uat-1:9092,kafka-uat-2:9092} (from application-rcs-dev.yml)

## External Services
- None found

---

# uclm-dlr-api-service Dependencies

## Databases
- None found

## Messaging / Brokers
- Kafka
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-email-netcore-dev.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-email-netcore-uat.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-rcs-dev.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-rcs-uat.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-sms-dev.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-sms-uat.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-wa-dev.yml)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092} (from application-wa-uat.yml)

## External Services
- None found

---

# uclm-dlr-enricher Dependencies

## Databases
- Aerospike

## Messaging / Brokers
- Kafka
- Topic: 60  # how often to check retry topic (from application-rcs-dev.yml)
- Topic: 60  # how often to check retry topic (from application-rcs-uat.yml)
- Topic: 60  # how often to check retry topic (from application.yml)
- Topic: cs_raw_reporting_topic (from application-email-netcore-dev.yml)
- Topic: cs_raw_reporting_topic (from application-email-netcore-uat.yml)
- Topic: cs_raw_reporting_topic (from application-rcs-dev.yml)
- Topic: cs_raw_reporting_topic (from application-rcs-uat.yml)
- Topic: cs_raw_reporting_topic (from application-sms-dev.yml)
- Topic: cs_raw_reporting_topic (from application-sms-uat.yml)
- Topic: cs_raw_reporting_topic (from application-wa-dev.yml)
- Topic: cs_raw_reporting_topic (from application-wa-uat.yml)
- Topic: cs_raw_reporting_topic (from application.yml)

## External Services
- None found

---

# uclm-orchestrator-service Dependencies

## Databases
- None found

## Messaging / Brokers
- Kafka
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092 (from application-dev.properties)
- Kafka Bootstrap Servers: kafka.principal.user= (from application-prod.properties)
- Kafka Bootstrap Servers: kafka.principal.user= (from application-sit.properties)
- Kafka Bootstrap Servers: kafka.principal.user= (from application-uat.properties)
- Kafka Bootstrap Servers: localhost:9092 (from application-local.properties)
- Topic: analytics (from application.properties)
- Topic: apb-log (from application.properties)
- Topic: apb-success-log (from application.properties)
- Topic: channel_partner_apb_err (from application-dev.properties)
- Topic: channel_partner_apb_err (from application-local.properties)
- Topic: channel_partner_apb_succ (from application-dev.properties)
- Topic: channel_partner_apb_succ (from application-local.properties)
- Topic: channel_partner_eml_nrt_svc_endpoint (from application-dev.properties)
- Topic: channel_partner_eml_nrt_svc_err (from application-dev.properties)
- Topic: channel_partner_eml_nrt_svc_err (from application-local.properties)
- Topic: channel_partner_eml_nrt_svc_succ (from application-dev.properties)
- Topic: channel_partner_eml_nrt_svc_succ (from application-local.properties)
- Topic: cs_raw_reporting_topic (from application-dev.properties)
- Topic: cs_raw_reporting_topic (from application-local.properties)
- Topic: dispatch (from application-local.properties)
- Topic: dispatch (from application.properties)
- Topic: dispatch-response (from application.properties)
- Topic: orchestrator-exceptions (from application-dev.properties)
- Topic: orchestrator-exceptions (from application-local.properties)
- Topic: orchestrator-exceptions (from application.properties)

## External Services
- Spring Cloud OpenFeign
- REST Endpoint: http://10.92.230.97:10200/cgi-bin/sendsms (from application.properties)
- REST Endpoint: https://channels-connect-api-apb.prd.adl.internal (from application-dev.properties)
- REST Endpoint: https://channels-connect-api-apb.prd.adl.internal (from application-local.properties)
- REST Endpoint: https://cms-dev.airtel.com (from application-dev.properties)
- REST Endpoint: https://cms-dev.airtel.com (from application-local.properties)
- REST Endpoint: https://emailapi.netcorecloud.net (from application-dev.properties)
- REST Endpoint: https://emailapi.netcorecloud.net (from application-local.properties)
- REST Endpoint: https://iqsms.airtel.in (from application-dev.properties)
- REST Endpoint: https://iqsms.airtel.in (from application-local.properties)
- REST Endpoint: https://iqwhatsapp.airtel.in (from application-dev.properties)
- REST Endpoint: https://iqwhatsapp.airtel.in (from application-local.properties)
- REST Endpoint: https://www.iqconversation.airtel.in (from application-dev.properties)
- REST Endpoint: https://www.iqconversation.airtel.in (from application-local.properties)
- REST Endpoint: https://www.iqconversation.airtel.in (from application.properties)

---

# uclm-rate-controller-service Dependencies

## Databases
- Aerospike
- MySQL
- PostgreSQL
- JDBC URL: jdbc:mysql://clm-aurora-mysql-auroradbcluster-9viezbzg6ngs.cluster-cl86omyi8hv8.ap-south-1.rds.amazonaws.com:3306/clmdbdev (from application-mysql-config.properties)
- JDBC URL: jdbc:postgresql://localhost:5432/postgres (from application-postgres-config.properties)

## Messaging / Brokers
- Kafka
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092 (from application-dev.properties)
- Kafka Bootstrap Servers: localhost:9092 (from application-local.properties)
- Kafka Bootstrap Servers: localhost:9092 (from application.properties)
- Topic: channel-partner-rate-controller-input (from application-dev.properties)
- Topic: comms-input (from application-local.properties)
- Topic: comms-input (from application.properties)
- Topic: event (from application-dev.properties)
- Topic: event (from application-local.properties)
- Topic: event (from application.properties)
- Topic: exceptions (from application-dev.properties)
- Topic: exceptions (from application-local.properties)
- Topic: exceptions (from application.properties)

## External Services
- None found

---

# uclm-validation-governance-service Dependencies

## Databases
- Aerospike
- Aerospike config: 3000 (from application-local.properties)
- Aerospike config: arch (from application-dev.properties)
- Aerospike config: test (from application-local.properties)

## Messaging / Brokers
- Kafka
- Kafka Bootstrap Servers: 10.248.244.115:9092,10.248.244.116:9092,10.248.244.117:9092 (from application-dev.properties)
- Kafka Bootstrap Servers: 10.92.36.44:9092,10.92.36.46:9092,10.92.36.48:9092 (from application-dev.properties)
- Kafka Bootstrap Servers: localhost:9092 (from application-local.properties)
- Topic: apb-exceptions (from application-dev.properties)
- Topic: cs_raw_reporting_topic (from application-dev.properties)
- Topic: cs_raw_reporting_topic (from application-local.properties)
- Topic: d2c-clm-sit (from application-dev.properties)
- Topic: dispatch (from application-dev.properties)
- Topic: dispatch (from application-local.properties)
- Topic: event (from application-dev.properties)
- Topic: event (from application-local.properties)
- Topic: exceptions (from application-dev.properties)
- Topic: exceptions (from application-local.properties)

## External Services
- REST Endpoint: http://10.222.160.29:2501/api/process/scrubbing/sms/single (from application-dev.properties)
- REST Endpoint: http://10.222.160.29:2501/api/process/scrubbing/sms/single (from application-local.properties)
- REST Endpoint: https://cms-dev.airtel.com/counter/increment (from application-dev.properties)
- REST Endpoint: https://cms-dev.airtel.com/counter/increment (from application-local.properties)

---

# uclm-analytics-reporting-service Dependencies

## Databases
- MySQL
- JDBC URL: jdbc:oracle:thin:@10.222.164.174:1535/NCHBDEV (from application-uat.properties)
- JDBC URL: jdbc:oracle:thin:@localhost:1521/XEPDB1 (from application-local.properties)

## Messaging / Brokers
- Kafka
- Kafka Bootstrap Servers: 10.222.201.101:9092,10.222.201.102:9092,10.222.201.103:9092 (from application-uat.properties)
- Kafka Bootstrap Servers: 10.92.36.48:9092,10.92.36.44:9092,10.92.36.46:9092 (from application-uat.properties)
- Kafka Bootstrap Servers: localhost:9092 (from application-local.properties)
- Kafka Bootstrap Servers: localhost:9092 (from application.properties)
- Topic: dimension_refresh_topic (from application-local.properties)
- Topic: dimension_refresh_topic (from application-uat.properties)
- Topic: dimension_refresh_topic (from application.properties)
- Topic: uclm_analytics (from application-local.properties)
- Topic: uclm_analytics (from application-uat.properties)
- Topic: uclm_analytics (from application.properties)
- Topic: uclm_campaign_status (from application-local.properties)
- Topic: uclm_campaign_status (from application-uat.properties)
- Topic: uclm_campaign_status (from application.properties)

## External Services
- REST Endpoint: http://authmanager-deployment.nextgenclm-api-develop.svc.cluster.local:7002 (from application-uat.properties)
- REST Endpoint: http://druid-uclm-uat.airtel.com (from application-local.properties)
- REST Endpoint: http://druid-uclm-uat.airtel.com (from application-uat.properties)
- REST Endpoint: https://<prod-auth-manager-host> (from application-prod.properties)
- REST Endpoint: https://authmanager-nextgenclm-api-develop.apps.n2ocp-dart-tclus-01.india.airtel.itm (from application-local.properties)
- REST Endpoint: https://localhost:3000,http://localhost:3000,https://adtechadrendering.wynk.in/ (from application-local.properties)

---

# uclm-campaign-audience-push Dependencies

## Databases
- MySQL
- JDBC URL: jdbc:oracle:thin:@10.222.164.174:1535/NCHBDEV (from application-uat.yml)
- JDBC URL: jdbc:oracle:thin:@localhost:1521/XEPDB1 (from application-local.yml)
- JDBC URL: jdbc:oracle:thin:@prod-db:1521/ORCL (from application-prod.yml)

## Messaging / Brokers
- None found

## External Services
- Spring WebFlux / WebClient
- REST Endpoint: https://am.airtel.in (from application-prod.yml)
- REST Endpoint: https://authmanager-nextgenclm-api-develop.apps.n2ocp-dart-tclus-01.india.airtel.itm (from application-local.yml)
- REST Endpoint: https://authmanager-nextgenclm-api-develop.apps.n2ocp-dart-tclus-01.india.airtel.itm (from application-uat.yml)
- REST Endpoint: https://uclm-audience-manager.apps.n2ocp-tclus-01.india.airtel.itm (from application-local.yml)
- REST Endpoint: https://uclm-audience-manager.apps.n2ocp-tclus-01.india.airtel.itm (from application-uat.yml)

---

# uclm-campaign-cg-exclusion Dependencies

## Databases
- MySQL
- JDBC URL: jdbc:oracle:thin:@10.222.164.174:1535/NCHBDEV (from application-prod.properties)
- JDBC URL: jdbc:oracle:thin:@10.222.164.174:1535/NCHBDEV (from application-uat.properties)

## Messaging / Brokers
- None found

## External Services
- None found

---

# uclm-campaign-data-file-dowload Dependencies

## Databases
- MySQL
- JDBC URL: jdbc:oracle:thin:@10.222.164.174:1535/NCHBDEV (from application-uat.yml)
- JDBC URL: jdbc:oracle:thin:@localhost:1521/XEPDB1 (from application-local.yml)

## Messaging / Brokers
- Kafka
- Kafka Bootstrap Servers: 10.92.36.48:9092,10.92.36.44:9092,10.92.36.46:9092} (from application-uat.yml)
- Kafka Bootstrap Servers: localhost:9092} (from application-local.yml)

## External Services
- REST Endpoint: http://authmanager-deployment.nextgenclm-api-develop.svc.cluster.local:7002 (from application-local.yml)
- REST Endpoint: http://authmanager-deployment.nextgenclm-api-develop.svc.cluster.local:7002 (from application-prod.yml)
- REST Endpoint: http://authmanager-deployment.nextgenclm-api-develop.svc.cluster.local:7002 (from application-uat.yml)

---

# uclm-campaign-exclusion-scan Dependencies

## Databases
- None found

## Messaging / Brokers
- None found

## External Services
- None found

---

# uclm-campaign-manager-event-enrichment Dependencies

## Databases
- MySQL
- JDBC URL: jdbc:oracle:thin:@10.222.164.174:1535/NCHBDEV (from application-dev.yml)

## Messaging / Brokers
- Kafka
- Kafka Bootstrap Servers: 10.92.36.48:9092,10.92.36.44:9092,10.92.36.46:9092,localhost:9092} (from application-dev.yml)
- Topic: cs_raw_reporting_topic (from application-dev.yml)

## External Services
- Spring Cloud OpenFeign
- REST Endpoint: http://campaign-manager-uclm-campaign-manager.nextgenclm-api-develop.svc.cluster.local:80 (from application.yml)
- REST Endpoint: http://cg-exclusion-service.nextgenclm-api-develop.svc.cluster.local:8080 (from application.yml)
- REST Endpoint: http://contentmanager-deployment.nextgenclm-api-develop.svc.cluster.local:7002/content-manager (from application.yml)
- REST Endpoint: http://exclusion-scan-service.nextgenclm-api-develop.svc.cluster.local:8080/ (from application.yml)
- REST Endpoint: https://uclm-audience-manager.apps.n2ocp-tclus-01.india.airtel.itm (from application.yml)

---

# uclm-campaign-manager Dependencies

## Databases
- MySQL
- JDBC URL: jdbc:oracle:thin:@10.222.164.174:1535/NCHBDEV (from application-prod.properties)
- JDBC URL: jdbc:oracle:thin:@10.222.164.174:1535/NCHBDEV (from application-uat.properties)
- JDBC URL: jdbc:oracle:thin:@localhost:1521/XEPDB1 (from application-local.properties)

## Messaging / Brokers
- Kafka
- Kafka Bootstrap Servers: 10.92.36.48:9092,10.92.36.44:9092,10.92.36.46:9092 (from application-uat.properties)
- Kafka Bootstrap Servers: localhost:9092 (from application-local.properties)
- Kafka Bootstrap Servers: prod-kafka:9092 (from application-prod.properties)
- Topic: orchestrator.request (from application.properties)
- Topic: uclm_analytics (from application-uat.properties)
- Topic: uclm_analytics (from application.properties)

## External Services
- REST Endpoint: http://cms-deployment.nextgenclm-api-develop.svc.cluster.local:7002 (from application-uat.properties)
- REST Endpoint: http://contentmanager-deployment.nextgenclm-api-develop.svc.cluster.local:7002/content-manager/api/v1/media/filter (from application-uat.properties)
- REST Endpoint: https://cms-dev.airtel.com (from application-local.properties)
- REST Endpoint: https://contentmanager-nextgenclm-api-develop.apps.n2ocp-dart-tclus-01.india.airtel.itm/content-manager/api/v1/media/filter (from application-local.properties)
- REST Endpoint: https://contentmanager-nextgenclm-api-develop.apps.n2ocp-dart-tclus-01.india.airtel.itm/content-manager/api/v1/media/filter (from application.properties)
- REST Endpoint: https://localhost:3000,http://localhost:3000,https://adtechadrendering.wynk.in (from application-local.properties)
- REST Endpoint: https://localhost:3000,http://localhost:3000,https://adtechadrendering.wynk.in (from application-prod.properties)
- REST Endpoint: https://localhost:3000,http://localhost:3000,https://adtechadrendering.wynk.in (from application-uat.properties)
- REST Endpoint: https://localhost:3000,http://localhost:3000,https://adtechadrendering.wynk.in (from application.properties)

---

# uclm-campaign-processor Dependencies

## Databases
- MySQL
- JDBC URL: jdbc:oracle:thin:@10.222.164.174:1535/NCHBDEV (from application-prod.properties)
- JDBC URL: jdbc:oracle:thin:@10.222.164.174:1535/NCHBDEV (from application-uat.properties)
- JDBC URL: jdbc:oracle:thin:@localhost:1521/XEPDB1 (from application-local.properties)

## Messaging / Brokers
- Kafka
- Kafka Bootstrap Servers: 10.92.36.48:9092,10.92.36.44:9092,10.92.36.46:9092 (from application-uat.properties)
- Kafka Bootstrap Servers: localhost:9092 (from application-local.properties)
- Kafka Bootstrap Servers: localhost:9092 (from application-prod.properties)

## External Services
- REST Endpoint: https://localhost:3000,http://localhost:3000,https://adtechadrendering.wynk.in/ (from application.properties)

---

# uclm-campaign-time-validation Dependencies

## Databases
- MySQL
- JDBC URL: jdbc:mysql://clm-aurora-mysql-auroradbcluster-9viezbzg6ngs.cluster-cl86omyi8hv8.ap-south-1.rds.amazonaws.com:3306/clmdbdev (from application-mysql.yml)
- JDBC URL: jdbc:oracle:thin:@10.222.164.174:1535/NCHBDEV (from application-oracle.yml)

## Messaging / Brokers
- Kafka
- Kafka Bootstrap Servers: 10.20.5.166:9092,10.20.5.177:9092,10.20.5.142:9092} (from application-prepod.yml)
- Kafka Bootstrap Servers: 10.20.5.166:9092,10.20.5.177:9092,10.20.5.142:9092} (from application-prod.yml)
- Kafka Bootstrap Servers: 10.20.5.166:9092,10.20.5.177:9092,10.20.5.142:9092} (from application-uat.yml)
- Kafka Bootstrap Servers: 10.92.36.48:9092,10.92.36.44:9092,10.92.36.46:9092} (from application-dev.yml)

## External Services
- Spring Cloud OpenFeign
- REST Endpoint: http://authmanager-deployment.nextgenclm-api-develop.svc.cluster.local:7002 (from application.yml)
- REST Endpoint: http://campaign-manager-uclm-campaign-manager.nextgenclm-api-develop.svc.cluster.local:80 (from application.yml)
- REST Endpoint: http://cg-exclusion-service.nextgenclm-api-develop.svc.cluster.local:8080/ (from application.yml)
- REST Endpoint: http://contentmanager-deployment.nextgenclm-api-develop.svc.cluster.local:7002/content-manager (from application.yml)
- REST Endpoint: http://exclusion-scan-service.nextgenclm-api-develop.svc.cluster.local:8080/ (from application.yml)

---

# uclm-test-campaign Dependencies

## Databases
- MySQL
- JDBC URL: jdbc:mysql://clm-aurora-mysql-auroradbcluster-9viezbzg6ngs.cluster-cl86omyi8hv8.ap-south-1.rds.amazonaws.com:3306/clmdbdev (from application-mysql.yml)
- JDBC URL: jdbc:oracle:thin:@10.222.110.100:1000/abcd (from application-uat.yml)
- JDBC URL: jdbc:oracle:thin:@10.222.120.100:1000/abcd (from application-prepod.yml)
- JDBC URL: jdbc:oracle:thin:@10.222.130.100:1000/abcd (from application-prod.yml)
- JDBC URL: jdbc:oracle:thin:@10.222.164.174:1535/NCHBDEV (from application-oracle.yml)

## Messaging / Brokers
- Kafka
- Kafka Bootstrap Servers: 10.92.36.48:9092,10.92.36.44:9092,10.92.36.46:9092} (from application-dev.yml)
- Topic: uat_email_topic (from application-uat.yml)
- Topic: uat_push_topic (from application-uat.yml)
- Topic: uat_rcs_topic (from application-uat.yml)
- Topic: uat_sms_topic (from application-uat.yml)
- Topic: uat_wa_topic (from application-uat.yml)

## External Services
- Spring Cloud OpenFeign
- REST Endpoint: http://exclusion-uat.apps.airtel.itm (from application-uat.yml)
- REST Endpoint: https://campaign-manager-nextgenclm-api-develop.apps.n2ocp-dart-tclus-01.india.airtel.itm (from application.yml)
- REST Endpoint: https://contentmanager-nextgenclm-api-develop.apps.n2ocp-dart-tclus-01.india.airtel.itm/content-manager (from application.yml)

---

# uclm-auth-manager Dependencies

## Databases
- Aerospike
- MySQL
- Aerospike config: 10.5.247.156:3000 (from application-dev.properties)
- Aerospike config: arch (from application-dev.properties)
- JDBC URL: jdbc:h2:mem:testdb (from application-h2.properties)
- JDBC URL: jdbc:mysql://<AURORA-ENDPOINT>:3306/<DB_NAME> (from application-aurora.properties)
- JDBC URL: jdbc:oracle:thin:@10.222.164.174:1535/NCHBDEV (from application-dev.properties)
- JDBC URL: jdbc:oracle:thin:@10.222.164.174:1535/NCHBDEV (from application-oracle.properties)

## Messaging / Brokers
- None found

## External Services
- REST Endpoint: http://uclm-test-campaign-service.nextgenclm-api-develop.svc.cluster.local:8092 (from application-dev.properties)
- REST Endpoint: https://adtechadrendering.wynk.in/login (from application.properties)
- REST Endpoint: https://uclm-test-campaign-nextgenclm-api-develop.apps.n2ocp-dart-tclus-01.india.airtel.itm (from application.properties)

---

# uclm-contentmgmt Dependencies

## Databases
- MongoDB

## Messaging / Brokers
- Kafka
- Kafka Bootstrap Servers: 10.92.36.48:9092,10.92.36.44:9092,10.92.36.46:9092 (from application-dev.properties)
- Kafka Bootstrap Servers: 10.92.36.48:9092,10.92.36.44:9092,10.92.36.46:9092 (from application-uat.properties)
- Kafka Bootstrap Servers: kafka.kerberos.enabled=true (from application.properties)
- Kafka Bootstrap Servers: localhost:9092 (from application-local.properties)
- Topic: content_change_log (from application-dev.properties)
- Topic: content_change_log_uat (from application-uat.properties)
- Topic: uclm_analytics (from application-dev.properties)
- Topic: uclm_analytics_uat (from application-uat.properties)

## External Services
- REST Endpoint: http://10.222.196.88/airtel-ucc-template-reg/template/create/external/v4 (from application-dev.properties)
- REST Endpoint: http://10.222.196.88/airtel-ucc-template-reg/template/create/external/v4 (from application-local.properties)
- REST Endpoint: http://10.222.196.88/airtel-ucc-template-reg/template/create/external/v4 (from application-uat.properties)
- REST Endpoint: http://10.223.74.55:10443 (from application.properties)
- REST Endpoint: https://iqconversation.airtel.in/gateway/airtel-xchange/rcs-content-manager/v1/rcs/template (from application-dev.properties)
- REST Endpoint: https://iqconversation.airtel.in/gateway/airtel-xchange/rcs-content-manager/v1/rcs/template (from application-local.properties)
- REST Endpoint: https://iqconversation.airtel.in/gateway/airtel-xchange/rcs-content-manager/v1/rcs/template (from application-uat.properties)
- REST Endpoint: https://iqconversation.airtel.in/gateway/airtel-xchange/whatsapp-content-manager/v1 (from application.properties)
- REST Endpoint: https://uclm-mcdn-preprod.wynk.in/ (from application.properties)

---

