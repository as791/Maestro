---
sidebar_position: 4
title: Lambda Deployment
---

# Deploying Autoscaler as AWS Lambda

Run your Maestro autoscaler as a serverless Lambda function triggered by EventBridge on a schedule.

## SAM Template

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31

Parameters:
  MaestroUrl:
    Type: String
    Description: Maestro API endpoint
  MaestroToken:
    Type: String
    NoEcho: true
  Environment:
    Type: String
    Default: prod
  Namespace:
    Type: String
    Default: streaming
  DeploymentName:
    Type: String

Resources:
  AutoscalerFunction:
    Type: AWS::Serverless::Function
    Properties:
      Runtime: python3.12
      Handler: autoscaler.handler
      Timeout: 30
      MemorySize: 128
      Environment:
        Variables:
          MAESTRO_URL: !Ref MaestroUrl
          MAESTRO_TOKEN: !Ref MaestroToken
          MAESTRO_ENV: !Ref Environment
          MAESTRO_NAMESPACE: !Ref Namespace
          MAESTRO_NAME: !Ref DeploymentName
          MIN_PARALLELISM: "2"
          MAX_PARALLELISM: "64"
          LAG_PER_SLOT: "50000"
      Events:
        Schedule:
          Type: ScheduleV2
          Properties:
            ScheduleExpression: rate(1 minute)
      Policies:
        - CloudWatchReadOnlyAccess  # for MSK lag metrics
```

## Lambda Handler

```python
# autoscaler.py
import math
import os
from maestro_sdk import MaestroClient, AutoscalerBase, ScaleDecision

class KafkaLagAutoscaler(AutoscalerBase):
    def __init__(self, client, env, ns, name):
        super().__init__(client, env, ns, name)
        self.min_p = int(os.environ.get("MIN_PARALLELISM", "2"))
        self.max_p = int(os.environ.get("MAX_PARALLELISM", "64"))
        self.lag_per_slot = int(os.environ.get("LAG_PER_SLOT", "50000"))

    def evaluate(self, status):
        health = status["currentVersion"]["healthSummary"]
        current = status["currentVersion"]["spec"]["parallelism"]
        lag = health.get("kafkaLag", 0)

        if lag > self.lag_per_slot:
            target = min(math.ceil(lag / self.lag_per_slot), self.max_p)
            if target > current:
                return ScaleDecision(target, reason=f"lag={lag:,}")

        if lag < self.lag_per_slot // 5 and current > self.min_p:
            target = max(current // 2, self.min_p)
            if target < current:
                return ScaleDecision(target, reason="lag low")

        return None

def handler(event, context):
    client = MaestroClient(
        os.environ["MAESTRO_URL"],
        token=os.environ.get("MAESTRO_TOKEN"),
    )
    scaler = KafkaLagAutoscaler(
        client,
        os.environ.get("MAESTRO_ENV", "prod"),
        os.environ.get("MAESTRO_NAMESPACE", "streaming"),
        os.environ.get("MAESTRO_NAME"),
    )
    result = scaler.execute()
    return {"scaled": result is not None, "result": result}
```

## Deploy

```bash
# Package
pip install maestro-flink-sdk -t package/
cp autoscaler.py package/
cd package && zip -r ../autoscaler.zip . && cd ..

# Deploy with SAM
sam deploy \
  --template-file template.yaml \
  --stack-name maestro-autoscaler-orders \
  --parameter-overrides \
    MaestroUrl=https://maestro.internal:8080 \
    MaestroToken=your-token \
    DeploymentName=orders \
  --capabilities CAPABILITY_IAM
```

## Kubernetes CronJob Alternative

If you don't want Lambda, deploy as a Kubernetes CronJob:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: maestro-autoscaler-orders
  namespace: maestro-system
spec:
  schedule: "*/1 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: autoscaler
            image: python:3.12-slim
            command: ["python", "-c"]
            args:
            - |
              import os, math
              from maestro_sdk import MaestroClient, AutoscalerBase, ScaleDecision
              
              class Scaler(AutoscalerBase):
                  def evaluate(self, status):
                      lag = status["currentVersion"]["healthSummary"].get("kafkaLag", 0)
                      current = status["currentVersion"]["spec"]["parallelism"]
                      target = min(max(math.ceil(lag / 50000), 2), 64)
                      if target != current:
                          return ScaleDecision(target, reason=f"lag={lag}")
                      return None
              
              client = MaestroClient(os.environ["MAESTRO_URL"])
              Scaler(client, os.environ["MAESTRO_ENV"], os.environ["MAESTRO_NAMESPACE"], os.environ["MAESTRO_NAME"]).execute()
            env:
            - name: MAESTRO_URL
              value: "http://maestro-api.maestro-system:8080"
            - name: MAESTRO_ENV
              value: "prod"
            - name: MAESTRO_NAMESPACE
              value: "streaming"
            - name: MAESTRO_NAME
              value: "orders"
          restartPolicy: OnFailure
```

## Monitoring

The Lambda function returns `{"scaled": true/false}` — set up a CloudWatch alarm on invocation errors and track scaling frequency via custom metrics.
