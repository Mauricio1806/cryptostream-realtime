# Runbook

## Deployment

### Local Development

```bash
make up           # Start Docker services
make bootstrap    # Create topics + buckets
make run-producer # Start Binance WebSocket producer
```

### Production (AWS)

1. Apply Terraform: `cd infra/terraform/envs/prod && terraform apply`
2. Deploy producer to ECS/EC2 with the `docker/producer.Dockerfile`
3. Submit Spark jobs via EMR Serverless API
4. Configure MWAA with DAGs from `airflow/dags/`

## SLAs

| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| End-to-end latency (Binance → Gold) | < 60s | > 120s |
| Producer uptime | 99.9% | WebSocket disconnect > 5min |
| Kafka consumer lag | < 10,000 messages | > 50,000 messages |
| Silver dedup accuracy | 100% (no duplicate event_ids) | Any duplicate |
| Gold freshness | < 2 minutes behind Silver | > 5 minutes |

## Troubleshooting

### Producer Issues

**WebSocket keeps disconnecting**
```bash
# Check producer logs
docker logs -f cs-producer 2>&1 | jq .

# Verify Binance endpoint is reachable
curl -I https://stream.binance.com:9443
```

**Kafka delivery failures**
```bash
# Check broker health
docker exec cs-kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check topic exists
docker exec cs-kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Spark Issues

**Checkpoint corruption**
```bash
# DANGER: Resets streaming state — data will be re-processed
aws s3 rm s3://cryptostream-prod-checkpoints/silver_trades/ --recursive

# Resubmit the job — it will start from latest offset
```

**Out of memory**
- Increase `spark.executor.memory` in spark-submit
- Reduce `maxOffsetsPerTrigger` to process smaller batches
- Check for data skew in partition key (symbol)

### Delta Lake Issues

**Too many small files**
```bash
# Run OPTIMIZE manually
spark.sql("OPTIMIZE delta.`s3a://cryptostream-dev-silver/trades/`")
```

**VACUUM not reclaiming space**
```bash
# Check retention setting
spark.sql("DESCRIBE DETAIL delta.`s3a://cryptostream-dev-silver/trades/`")

# Force vacuum with shorter retention (DANGER in prod)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
```

## On-Call Procedures

1. **Page received** → Check CloudWatch dashboard for the alerting metric
2. **Identify layer** → Is it producer, Kafka, Spark, or storage?
3. **Check logs** → CloudWatch Log Insights query by correlation_id
4. **Mitigate** → Restart the failing component (producer restart is safe; Spark restarts from checkpoint)
5. **Root cause** → Document in incident channel within 24h
