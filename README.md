OpenSearch:
docker pull opensearchproject/opensearch
docker run -d -p 9200:9200 -p 9600:9600 -e "discovery.type=single-node" -e "plugins.security.disabled=true" -e "compatibility.override_main_response_version=true" opensearchproject/opensearch:latest

OpenSearch-dashboards:
docker pull opensearchproject/opensearch-dashboards
docker run -d -p 5601:5601 -e "OPENSEARCH_HOSTS=http://{ip}:9200" -e "DISABLE_SECURITY_DASHBOARDS_PLUGIN=true" opensearchproject/opensearch-dashboards:latest

Read in dev-tools:
http://{ip}:5601/app/dev_tools#/console
GET /wikimedia/_doc/3rv7mX8Bsl8SqPGnCb3O