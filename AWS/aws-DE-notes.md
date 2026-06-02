# 🚀 AWS for Data Engineers — Comprehensive Study Notes

> **Purpose:** Interview preparation & deep understanding of AWS services for Data Engineering roles.
> **Coverage:** S3, IAM, EC2, ECR, Lambda, Glue, Athena, Redshift, SNS, CloudWatch, Step Functions, Boto3, EventBridge, and more.

---

## 📑 Table of Contents

1. [Cloud Fundamentals — IaaS, PaaS, SaaS](#1-cloud-fundamentals)
2. [AWS Global Infrastructure — Regions, AZs, Edge Locations](#2-aws-global-infrastructure)
3. [AWS Account & Organizations](#3-aws-account--organizations)
4. [Amazon S3](#4-amazon-s3)
5. [IAM — Identity & Access Management](#5-iam--identity--access-management)
6. [Amazon EC2](#6-amazon-ec2)
7. [Amazon ECR — Elastic Container Registry](#7-amazon-ecr)
8. [AWS Lambda](#8-aws-lambda)
9. [AWS Glue](#9-aws-glue)
10. [Amazon Athena](#10-amazon-athena)
11. [Amazon Redshift](#11-amazon-redshift)
12. [Amazon SNS](#12-amazon-sns)
13. [Amazon CloudWatch](#13-amazon-cloudwatch)
14. [AWS Step Functions](#14-aws-step-functions)
15. [Amazon EventBridge](#15-amazon-eventbridge)
16. [Boto3 — AWS SDK for Python](#16-boto3--aws-sdk-for-python)
17. [Data Lake Architecture on AWS](#17-data-lake-architecture-on-aws)
18. [Most Asked Interview Questions](#18-most-asked-interview-questions)

---

## 1. Cloud Fundamentals

### IaaS, PaaS, SaaS

The three fundamental cloud delivery models determine **how much the provider manages vs. how much you manage**.

```mermaid
graph TD
    A[Cloud Service Models] --> B[IaaS]
    A --> C[PaaS]
    A --> D[SaaS]

    B --> B1["You manage: OS, Runtime, App, Data
    Provider: Hardware, Network, Virtualization
    AWS Examples: EC2, S3, VPC"]

    C --> C1["You manage: App, Data
    Provider: OS, Runtime, Scaling, Patching
    AWS Examples: Elastic Beanstalk, RDS, Lambda"]

    D --> D1["You manage: Data & Settings only
    Provider: Everything
    AWS Examples: WorkMail, Chime"]
```

| Layer | IaaS | PaaS | SaaS |
|---|---|---|---|
| Application | You | You | Provider |
| Runtime / OS | You | Provider | Provider |
| Virtualization | Provider | Provider | Provider |
| Hardware | Provider | Provider | Provider |
| **Control** | High | Medium | Low |
| **Responsibility** | High | Medium | Low |

#### 🍕 Pizza Analogy

| Model | Analogy |
|---|---|
| On-Premise | Make pizza from scratch at home |
| IaaS | Buy dough & ingredients, rent an oven |
| PaaS | Buy a pizza base, just add toppings |
| SaaS | Order from Domino's |

---

## 2. AWS Global Infrastructure

### Regions

An **AWS Region** is a physical geographic cluster of data centers, completely independent of other regions.

```mermaid
graph TD
    AWS[AWS Global Infrastructure] --> R1[Region: ap-south-1 Mumbai]
    AWS --> R2[Region: us-east-1 N. Virginia]
    AWS --> R3[Region: eu-central-1 Frankfurt]

    R1 --> AZ1[AZ: ap-south-1a]
    R1 --> AZ2[AZ: ap-south-1b]
    R1 --> AZ3[AZ: ap-south-1c]

    AZ1 --> DC1[Data Center]
    AZ2 --> DC2[Data Center]
    AZ3 --> DC3[Data Center]
```

**How to Choose a Region:**
1. **Compliance & Data Residency** — legal requirements (GDPR → `eu-*`)
2. **Latency** — closest to your users
3. **Service Availability** — not all services in all regions; `us-east-1` gets new services first
4. **Pricing** — varies by region; `us-east-1` typically cheapest

### Availability Zones (AZs)

- Each region has **3+ AZs**, each an isolated data center cluster
- Connected via **low-latency private fiber**
- Independent power, cooling, networking
- If one AZ fails, others keep running → **fault tolerance**

### Edge Locations

- **400+ edge locations** across 90+ cities (vs 33+ regions)
- Purpose: **cache and deliver content closer to users**
- Power **Amazon CloudFront** (CDN)

```mermaid
graph LR
    User["User (Mumbai)"] --> EL["Edge Location (Mumbai)"]
    EL -->|Cache HIT| User
    EL -->|Cache MISS| Origin["Origin Server (us-east-1)"]
    Origin --> EL
```

**Services using Edge Locations:**

| Service | Role at Edge |
|---|---|
| **CloudFront** | CDN — cache static/dynamic content |
| **Route 53** | DNS resolution closest to user |
| **AWS WAF & Shield** | DDoS protection at the edge |
| **Lambda@Edge** | Run Lambda at edge, customize CloudFront responses |
| **Global Accelerator** | Route via AWS private backbone |

---

## 3. AWS Account & Organizations

### What is an AWS Account?

The **fundamental container** for all resources, identities, and billing. Everything you create lives inside it.

```mermaid
graph TD
    Account[AWS Account] --> Identity[Identity Layer]
    Account --> Resources[Resources Layer]
    Account --> Billing[Billing Layer]

    Identity --> RootUser[Root User - All powerful]
    Identity --> IAMUsers[IAM Users]
    Identity --> IAMRoles[IAM Roles]

    Resources --> EC2[EC2]
    Resources --> S3[S3]
    Resources --> RDS[RDS]
    Resources --> Lambda[Lambda]
```

### Root User

| Property | Detail |
|---|---|
| Login | Email + password from signup |
| Access | Unlimited — can do anything |
| Daily Use | ❌ NEVER use for day-to-day |
| Best Practice | Enable MFA, then lock it away |

**Tasks only Root can do:**
- Close/delete the AWS account
- Change account name or root email
- Change AWS support plan
- Enable MFA Delete on S3

### AWS Organizations — Multi-Account Strategy

```mermaid
graph TD
    MgmtAccount[Management Account] --> OUProd[OU: Production]
    MgmtAccount --> OUDev[OU: Development]
    MgmtAccount --> OUSec[OU: Security]

    OUProd --> ProdApp[Account: Prod-App]
    OUProd --> ProdData[Account: Prod-Data]
    OUDev --> DevApp[Account: Dev-App]
    OUSec --> Audit[Account: Audit-Logs]
```

**Benefits:**
- **Blast radius isolation** — dev mistakes don't affect prod
- **Consolidated billing** — one bill, volume discounts
- **SCPs (Service Control Policies)** — org-level guardrails overriding even admin permissions

---

## 4. Amazon S3

### What is S3?

**Simple Storage Service** — infinitely scalable **object storage**. Store any file (object) in buckets. Not a file system — objects are accessed via keys (paths).

```mermaid
graph TD
    S3[Amazon S3] --> Bucket1[Bucket: my-data-lake]
    S3 --> Bucket2[Bucket: my-backups]

    Bucket1 --> Obj1["raw/orders/2024/01/orders.parquet"]
    Bucket1 --> Obj2["curated/customers/customers.parquet"]
    Bucket1 --> Obj3["archive/2022/logs.gz"]
```

**Key Properties:**
- Buckets are **globally unique** names
- Objects can be up to **5TB** in size
- **11 9s durability** (99.999999999%)
- Accessed via **HTTPS URLs** or AWS SDK

---

### S3 Storage Classes

| Storage Class | Access Freq | Retrieval Speed | Min Duration | Key Detail |
|---|---|---|---|---|
| **Standard** | Frequent | Milliseconds | None | Default, highest cost |
| **Intelligent-Tiering** | Unknown | Milliseconds | None | Auto-moves between tiers |
| **Standard-IA** | Infrequent | Milliseconds | 30 days | Retrieval fee applies |
| **One Zone-IA** | Infrequent | Milliseconds | 30 days | Single AZ — data lost if AZ fails |
| **Glacier Instant** | Rare | Milliseconds | 90 days | Archive with instant retrieval |
| **Glacier Flexible** | Very Rare | 1min–12hrs | 90 days | Expedited/Standard/Bulk retrieval |
| **Glacier Deep Archive** | Almost Never | 12–48 hrs | 180 days | Cheapest — long-term retention |

---

### S3 Versioning

Keeps **multiple versions** of the same object. Think of it as Git for S3.

```mermaid
sequenceDiagram
    participant User
    participant S3

    User->>S3: Upload report.csv → Version v1
    User->>S3: Upload report.csv → Version v2 (latest)
    User->>S3: DELETE report.csv
    S3-->>S3: Adds Delete Marker (v3) — data NOT deleted
    User->>S3: Delete the Delete Marker
    S3-->>User: File restored ✅
```

**Key Points:**
- Once enabled, **cannot be fully disabled** (only suspended)
- Deletion adds a **Delete Marker** — old versions remain
- **MFA Delete** (root only) requires MFA to permanently delete versions
- Each version billed separately

---

### S3 Encryption

```mermaid
graph TD
    Enc[S3 Encryption] --> AtRest[At Rest]
    Enc --> InTransit[In Transit - HTTPS/TLS]

    AtRest --> SSE-S3["SSE-S3: AWS manages keys - AES-256 - Default"]
    AtRest --> SSE-KMS["SSE-KMS: You manage keys via KMS - CloudTrail audit trail"]
    AtRest --> SSE-C["SSE-C: You provide key per request - AWS never stores it"]
    AtRest --> CSE["Client-Side: You encrypt before sending - AWS sees nothing"]
```

| Type | Key Managed By | Audit Trail | Use Case |
|---|---|---|---|
| SSE-S3 | AWS | ❌ | Default, simple |
| SSE-KMS | AWS KMS (you configure) | ✅ CloudTrail | Compliance, auditing |
| SSE-C | You | ❌ | Full key control |
| CSE | You | ❌ | Max security |

---

### S3 Lifecycle Policies

Automate transitions between storage classes and expiration of objects.

```mermaid
graph LR
    D0["Day 0: Upload → Standard"] --> D30["Day 30 → Standard-IA"]
    D30 --> D90["Day 90 → Glacier Flexible"]
    D90 --> D365["Day 365 → Glacier Deep Archive"]
    D365 --> D2555["Day 2555 → DELETE"]
```

**Rule Targets:**
- Entire bucket
- Specific prefix (`raw/2022/*`)
- Specific tags (`env=dev`)

**Versioning + Lifecycle:**
- Non-current versions → move to Glacier after 30 days
- Delete markers → clean up after 1 day
- Incomplete multipart uploads → delete after 7 days

---

### S3 Replication

```mermaid
graph LR
    subgraph "Region: ap-south-1"
        BucketA["Bucket A (Source)"]
    end
    subgraph "Region: us-east-1"
        BucketB["Bucket B (Destination)"]
    end

    BucketA -->|"CRR (Cross-Region)"| BucketB
```

| Feature | CRR | SRR |
|---|---|---|
| Regions | Different | Same |
| Use case | DR, compliance, global latency | Log aggregation, dev/prod sync |
| Requirement | Versioning on both buckets | Versioning on both buckets |

**What IS replicated:** New objects, metadata, tags, ACLs, SSE objects
**What is NOT replicated:** Existing objects (use Batch Replication), delete markers (by default), lifecycle actions

**Replication Time Control (RTC):** Guarantees 95% of objects replicated within **15 minutes** — extra cost.

---

### S3 Interview Questions

> **Q: What is the difference between S3 Standard-IA and One Zone-IA?**
> Standard-IA stores data across multiple AZs (99.9% availability), while One Zone-IA stores in a single AZ (99.5% availability). One Zone-IA is ~20% cheaper but data is permanently lost if the AZ is destroyed.

> **Q: How does S3 versioning protect against accidental deletions?**
> Deleting an object with versioning enabled only adds a Delete Marker — all previous versions remain. To permanently delete, you must explicitly delete a specific version ID.

> **Q: What are the ways to secure an S3 bucket?**
> Block Public Access settings, Bucket Policies (resource-based), ACLs (legacy), IAM Policies (identity-based), VPC Endpoints, encryption (SSE-KMS), MFA Delete, S3 Object Lock.

> **Q: What is S3 Object Lock?**
> Prevents objects from being deleted or overwritten for a fixed time period or indefinitely. Supports WORM (Write Once Read Many) compliance. Two modes: **Governance** (admin can override) and **Compliance** (no one can override — not even root).

> **Q: How do you optimize Athena query costs on S3?**
> Use **columnar format (Parquet/ORC)**, **partition** data by date/region, **compress** files (Snappy/GZIP). These reduce the amount of data scanned.

---

## 5. IAM — Identity & Access Management

### What is IAM?

IAM is AWS's **centralized identity and access control** service. Controls **who** (authentication) can do **what** (authorization) on **which resources**.

```mermaid
graph TD
    IAM[AWS IAM] --> Users[IAM Users - Long-term credentials]
    IAM --> Groups[IAM Groups - Collection of users]
    IAM --> Roles[IAM Roles - Temporary credentials]
    IAM --> Policies[IAM Policies - Permission documents]

    Users --> Groups
    Groups --> Policies
    Roles --> Policies
    Users --> Policies
```

**IAM is Global** — not region-specific.

---

### IAM Components

#### Users
Long-lived identities for **people or applications**.
- **Console access:** username + password
- **Programmatic access:** Access Key ID + Secret Access Key

#### Groups
Logical collection of users — attach policies to groups, not individual users.

```
Group: DataEngineers
├── User: hemant
├── User: priya
└── User: rahul
→ All inherit the same policies
```

#### Roles
**Temporary credentials** assumed by trusted entities. No long-lived secrets.

```mermaid
graph LR
    EC2["EC2 Instance"] -->|"Assumes Role"| Role["IAM Role: EC2-S3-ReadRole"]
    Lambda["Lambda Function"] -->|"Assumes Role"| LRole["IAM Role: Lambda-GlueRole"]
    Role --> Policy["Policy: s3:GetObject on my-bucket/*"]
    LRole --> LPolicy["Policy: glue:StartJobRun"]
```

**Who can assume roles:**
- AWS services (EC2, Lambda, Glue, ECS, etc.)
- IAM users (cross-account access)
- External identity providers (SAML, OIDC — federated users)

#### Policies
JSON documents defining **allow/deny permissions**.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::my-data-lake/*"
    },
    {
      "Effect": "Deny",
      "Action": "s3:DeleteObject",
      "Resource": "*"
    }
  ]
}
```

---

### Policy Types

| Policy Type | Attached To | Description |
|---|---|---|
| **Identity-based** | Users, Groups, Roles | What this identity can do |
| **Resource-based** | S3 buckets, SQS queues, KMS keys | Who can access this resource |
| **Permission Boundaries** | Users, Roles | Maximum permissions cap |
| **SCPs (Service Control Policies)** | AWS Org OUs/Accounts | Hard limits across entire org |
| **Session Policies** | Assumed role sessions | Restrict a specific role session |

---

### IAM Policy Evaluation Logic

```mermaid
graph TD
    Request[Incoming Request] --> Deny{Explicit DENY?}
    Deny -->|Yes| DenyResult[❌ DENIED]
    Deny -->|No| Allow{Explicit ALLOW?}
    Allow -->|Yes| AllowResult[✅ ALLOWED]
    Allow -->|No| ImplicitDeny[❌ IMPLICIT DENY - Default]
```

> **Rule:** Explicit DENY always wins. Everything is denied by default unless explicitly allowed.

---

### IAM Best Practices

1. **Least Privilege** — grant only permissions needed, nothing more
2. **Enable MFA** — on root and all IAM users
3. **Use Roles over Users** — for AWS service access
4. **Rotate Credentials** — regularly rotate access keys
5. **Never use Root** — for day-to-day operations
6. **Use Groups** — manage permissions via groups, not individual users
7. **Use Permission Boundaries** — limit max permissions of users/roles
8. **Audit with Access Analyzer** — identify unintended public/cross-account access

---

### IAM for Data Engineers — Common Roles

```
Glue ETL Job Role:
├── s3:GetObject, s3:PutObject on data lake bucket
├── glue:GetTable, glue:UpdateTable (Data Catalog)
├── logs:CreateLogGroup, logs:PutLogEvents (CloudWatch)
└── kms:Decrypt, kms:GenerateDataKey (if encrypted)

Lambda Function Role:
├── s3:GetObject (read trigger file)
├── glue:StartJobRun (trigger Glue)
├── sqs:SendMessage (notify queue)
└── cloudwatch:PutMetricData (publish metrics)

Redshift Cluster Role:
├── s3:GetObject (COPY command)
├── s3:ListBucket
└── glue:GetTable (Spectrum external tables)
```

---

### IAM Interview Questions

> **Q: What is the difference between an IAM Role and IAM User?**
> An IAM User has **permanent long-term credentials** (password, access keys) for a specific person/app. An IAM Role has **temporary credentials** (STS tokens, 15min–12hrs) assumed by trusted entities. Roles are preferred for AWS services — no hardcoded secrets.

> **Q: What is the principle of least privilege?**
> Grant only the minimum permissions required to perform a task, nothing more. If a Lambda function only reads from S3, give it only `s3:GetObject` — not `s3:*`.

> **Q: What is a Permission Boundary?**
> An advanced IAM feature that sets the maximum permissions a user or role can ever have — even if policies grant more. It acts as a ceiling. Example: developers can create roles, but the Permission Boundary ensures those roles can never access production resources.

> **Q: How does IAM PassRole work?**
> `iam:PassRole` permission is needed when a service (like Glue or Lambda) needs to assume an IAM role. Without it, you cannot create a Glue job that uses a specific IAM role, even if you have Glue permissions.

> **Q: What is STS (Security Token Service)?**
> AWS STS issues **temporary security credentials** — Access Key, Secret Key, Session Token — that expire automatically. Used when roles are assumed.

---

## 6. Amazon EC2

### What is EC2?

**Elastic Compute Cloud** — virtual servers (instances) in the cloud. The backbone of AWS compute.

```mermaid
graph TD
    EC2[Amazon EC2] --> Instance[EC2 Instance = Virtual Machine]
    Instance --> OS[Operating System: Linux/Windows]
    Instance --> Storage[Storage: EBS / Instance Store]
    Instance --> Network[Network: VPC, Security Groups, ENI]
    Instance --> IAMRole[IAM Role for permissions]
```

---

### Instance Types

Named by family, generation, size: `t3.micro`, `m5.xlarge`, `r6i.2xlarge`

| Family | Optimized For | Examples |
|---|---|---|
| **t** (Burstable) | Low cost, variable workloads | t3.micro, t3.medium |
| **m** (General) | Balanced compute/memory | m5.large, m6i.xlarge |
| **c** (Compute) | CPU-intensive | c5.xlarge, c6g.2xlarge |
| **r** (Memory) | Memory-intensive | r5.xlarge, r6i.4xlarge |
| **i** (Storage) | NVMe SSD, I/O intensive | i3.large, i3en.xlarge |
| **p/g** (GPU) | ML training, HPC | p3.2xlarge, g4dn.xlarge |

---

### Purchasing Options

| Option | Best For | Savings |
|---|---|---|
| **On-Demand** | Short-term, unpredictable | None — pay per second |
| **Reserved (1 or 3yr)** | Steady, predictable workloads | Up to 72% off on-demand |
| **Spot** | Fault-tolerant, flexible jobs | Up to 90% off — can be interrupted |
| **Savings Plans** | Flexible reserved-like pricing | Up to 66% |
| **Dedicated Host** | Compliance, bring-your-own-license | Most expensive |

> **For data engineering:** EMR Task Nodes on **Spot** for big cost savings. Use **Reserved** for always-on Redshift.

---

### Security Groups

Virtual **firewalls** for EC2 instances — control inbound and outbound traffic.

```
Security Group: data-pipeline-sg
Inbound Rules:
  Port 22 (SSH)   ← My IP only
  Port 5439       ← Redshift, from BI tool SG only
Outbound Rules:
  ALL traffic → 0.0.0.0/0 (all destinations)
```

**Key Properties:**
- **Stateful** — if inbound is allowed, response is automatically allowed
- Default: deny all inbound, allow all outbound
- Multiple SGs can be attached to one instance

---

### EBS — Elastic Block Store

Network-attached **block storage** for EC2. Think of it as a virtual hard disk.

```mermaid
graph LR
    EC2A["EC2 (AZ: 1a)"] -->|attached| EBS1["EBS Volume 100GB (AZ: 1a)"]
    EC2B["EC2 (AZ: 1b)"] -->|attached| EBS2["EBS Volume 200GB (AZ: 1b)"]
    EBS1 -->|snapshot| S3[(S3 Snapshot)]
    S3 -->|restore to any AZ| EBS3["EBS Volume (any AZ)"]
```

| EBS Type | Use Case | Max IOPS |
|---|---|---|
| **gp3** (recommended) | General purpose SSD | 16,000 |
| **io1/io2** | High-perf databases | 64,000 |
| **st1** | Big data, throughput HDD | 500 MB/s |
| **sc1** | Cold, infrequent access | 250 MB/s |

---

### EFS — Elastic File System

Managed **NFS shared file system** — multiple EC2s can mount it simultaneously.

```mermaid
graph TD
    EFS[EFS File System - Multi-AZ] --> MT1[Mount Target AZ-1a]
    EFS --> MT2[Mount Target AZ-1b]
    MT1 --> EC2A[EC2 Instance A]
    MT1 --> EC2B[EC2 Instance B]
    MT2 --> EC2C[EC2 Instance C]
    MT2 --> Lambda[Lambda Function]
```

| | EBS | EFS |
|---|---|---|
| Attachment | 1 EC2 (default) | 1000s simultaneously |
| AZ Scope | Single AZ | Multi-AZ (Regional) |
| Scaling | Manual | Automatic |
| OS | Linux + Windows | Linux only |
| Use Case | DB volumes, OS drives | Shared workloads, ML data |

---

### EC2 Interview Questions

> **Q: What is the difference between stopping and terminating an EC2 instance?**
> **Stop:** Shuts down the instance, preserves state and EBS volumes, no compute charges (EBS charges continue). Can be restarted. **Terminate:** Permanently deletes the instance. Root EBS is deleted by default. Cannot be restarted.

> **Q: What is an AMI?**
> Amazon Machine Image — a pre-configured template that includes OS, software, and configuration needed to launch an EC2 instance. Used to clone instances, create launch templates, and share configurations.

> **Q: What are Spot Instances and when do you use them in data engineering?**
> Spot instances use spare AWS capacity at up to 90% discount, but can be interrupted with 2-minute warning. Use for: EMR Task Nodes (Spark jobs), batch processing, fault-tolerant data pipelines where jobs can be retried.

---

## 7. Amazon ECR

### What is ECR?

**Elastic Container Registry** — fully managed Docker/OCI container image registry. The AWS equivalent of Docker Hub — store, manage, and deploy container images.

```mermaid
graph LR
    Dev["Developer/CI-CD"] -->|docker push| ECR[ECR Repository]
    ECR -->|pull image| Lambda["Lambda (Container)"]
    ECR -->|pull image| ECS["ECS Task"]
    ECR -->|pull image| EKS["EKS Pod"]
    ECR -->|pull image| Glue["Glue Custom Job"]
```

---

### Key Concepts

#### Repository
A named store for Docker images — similar to a GitHub repo but for containers.

```
ECR Registry (account-level)
└── Repository: data-pipeline/etl-job
        ├── Image: latest
        ├── Image: v1.0.0
        └── Image: v1.1.0
```

#### Image Tags & URIs

```
<account-id>.dkr.ecr.<region>.amazonaws.com/<repo-name>:<tag>

Example:
123456789.dkr.ecr.ap-south-1.amazonaws.com/data-pipeline/etl-job:v1.0.0
```

---

### ECR Features

| Feature | Description |
|---|---|
| **Private Registry** | Default — only accessible within your AWS account |
| **Public Registry** | `public.ecr.aws` — share images publicly |
| **Image Scanning** | Scan for vulnerabilities (CVEs) on push or on-demand |
| **Lifecycle Policies** | Auto-delete old/untagged images to save storage cost |
| **Replication** | Cross-region and cross-account image replication |
| **Encryption** | Images encrypted at rest with KMS |
| **Immutable Tags** | Prevent image tags from being overwritten |

---

### Docker + ECR Workflow

```bash
# 1. Authenticate Docker to ECR
aws ecr get-login-password --region ap-south-1 | \
  docker login --username AWS --password-stdin \
  123456789.dkr.ecr.ap-south-1.amazonaws.com

# 2. Build your image
docker build -t data-pipeline/etl-job .

# 3. Tag it for ECR
docker tag data-pipeline/etl-job:latest \
  123456789.dkr.ecr.ap-south-1.amazonaws.com/data-pipeline/etl-job:latest

# 4. Push to ECR
docker push \
  123456789.dkr.ecr.ap-south-1.amazonaws.com/data-pipeline/etl-job:latest
```

---

### ECR in Data Engineering

**Use Cases:**
- Package **Glue custom connectors** as containers
- Run **Lambda functions as containers** (up to 10GB image vs 50MB zip)
- Deploy **custom Spark jobs** on ECS/EKS
- Standardize data pipeline environments across dev/staging/prod

---

### ECR Interview Questions

> **Q: Why use ECR over Docker Hub for AWS workloads?**
> ECR integrates natively with AWS IAM (no separate credentials), runs inside your VPC (no internet for pulls), provides lower latency since images stay in the same region, and eliminates Docker Hub rate limits.

> **Q: How do you secure ECR repositories?**
> IAM resource-based policies control who can push/pull. Enable image tag immutability to prevent overwrites. Enable vulnerability scanning. Use KMS encryption. Keep private repositories (avoid public unless needed).

> **Q: What is the difference between ECR private and public registries?**
> **Private:** Accessible only within your AWS account and via explicit cross-account IAM grants. **Public:** Images accessible to anyone via `public.ecr.aws` — for sharing open-source tools.

---

## 8. AWS Lambda

### What is Lambda?

Lambda is AWS's **serverless compute** service — run code in response to events without provisioning or managing servers. AWS handles everything: servers, scaling, patching, availability.

```mermaid
graph LR
    Trigger["Trigger / Event Source"] -->|invokes| Lambda["Lambda Function"]
    Lambda -->|uses| Role["IAM Execution Role"]
    Lambda -->|reads/writes| AWS["AWS Services: S3, DynamoDB, RDS..."]
    Lambda -->|logs to| CW["CloudWatch Logs"]
```

---

### Key Properties

| Property | Detail |
|---|---|
| **Max execution time** | 15 minutes per invocation |
| **Memory** | 128MB to 10,240MB (10GB) |
| **CPU** | Proportional to memory allocation |
| **Ephemeral storage** | 512MB to 10GB at `/tmp` |
| **Deployment package** | .zip (50MB) or Container image (10GB) |
| **Concurrency** | 1000 concurrent executions by default (can increase) |
| **Pricing** | Per GB-second of compute + per request |

---

### Lambda Invocation Models

```mermaid
graph TD
    Inv[Invocation Types] --> Sync[Synchronous]
    Inv --> Async[Asynchronous]
    Inv --> Stream[Event Source Mapping - Stream/Queue]

    Sync --> SyncEx["API Gateway, Lambda → Lambda
    Caller waits for response
    Errors returned to caller"]

    Async --> AsyncEx["S3 Events, SNS, EventBridge
    Lambda queues the event
    Retries 2 times on failure
    Dead Letter Queue for failures"]

    Stream --> StreamEx["SQS, Kinesis, DynamoDB Streams
    Lambda polls the source
    Batch processing
    Checkpointing on streams"]
```

---

### Cold Start vs Warm Start

```mermaid
sequenceDiagram
    participant Event
    participant Lambda

    Note over Event,Lambda: COLD START (first invocation or scaled out)
    Event->>Lambda: Invoke
    Lambda->>Lambda: Download code package
    Lambda->>Lambda: Start execution environment (container)
    Lambda->>Lambda: Initialize runtime
    Lambda->>Lambda: Run INIT code (outside handler)
    Lambda->>Lambda: Execute handler()
    Lambda-->>Event: Response (slower ⏱️)

    Note over Event,Lambda: WARM START (reuse existing container)
    Event->>Lambda: Invoke
    Lambda->>Lambda: Execute handler() only
    Lambda-->>Event: Response (fast ⚡)
```

**Reducing Cold Starts:**
- **Provisioned Concurrency** — keep N environments pre-initialized (no cold start)
- **SnapStart** (Java) — snapshot initialized environment, restore on invocation
- Smaller deployment packages → faster initialization
- Avoid heavy imports in handler (use module-level imports)

---

### Lambda Execution Role

Every Lambda function must have an **IAM Execution Role** — what the function can access.

```python
# Function execution role gives access to S3 and Glue
# No hardcoded credentials needed!

import boto3

def handler(event, context):
    s3 = boto3.client('s3')           # Uses execution role credentials
    glue = boto3.client('glue')       # Same role
    
    # Read from S3
    obj = s3.get_object(Bucket='my-bucket', Key='data/file.parquet')
    
    # Trigger Glue job
    glue.start_job_run(JobName='my-etl-job')
    
    return {"status": "success"}
```

---

### Lambda in Data Pipelines

```mermaid
graph TD
    S3Upload["New file lands in S3"] -->|S3 Event| Lambda1["Lambda: Validate & Route"]
    Lambda1 -->|valid| GlueJob["Glue ETL Job"]
    Lambda1 -->|invalid| SNS["SNS: Alert team"]
    GlueJob -->|complete| EventBridge["EventBridge: Job Succeeded"]
    EventBridge --> Lambda2["Lambda: Update partition in Glue Catalog"]
    EventBridge --> SQS["SQS: Notify downstream teams"]
```

**Common Lambda Use Cases in Data Engineering:**
- Trigger Glue jobs when new S3 files arrive
- Validate file schema before processing
- Update Glue Data Catalog partitions
- Orchestrate small transformations
- Call Athena queries programmatically
- Send alerts when pipeline steps fail
- Clean up temporary S3 files post-processing

---

### Lambda Layers

Reusable libraries packaged separately from function code:

```
Lambda Function (small, just your code)
└── Layer 1: pandas, numpy, pyarrow (shared across 50 functions)
└── Layer 2: custom utilities library
```

Up to **5 layers** per function. Max 250MB total (unzipped).

---

### Lambda Concurrency

```
Account Concurrency Limit: 1000 (default, can increase)

Reserved Concurrency:
  → Guarantees N executions always available for a function
  → Also acts as a throttle cap for that function

Provisioned Concurrency:
  → Pre-initialized execution environments
  → Eliminates cold starts for latency-sensitive functions
  → Billed even when not invoked
```

---

### Lambda Interview Questions

> **Q: What is the maximum execution timeout for a Lambda function?**
> 15 minutes (900 seconds). For longer-running tasks, use AWS Glue, EMR, ECS, or Step Functions.

> **Q: How do you handle Lambda cold starts?**
> Use **Provisioned Concurrency** to pre-warm environments. Use **SnapStart** for Java. Keep deployment packages small. Move heavy initialization code outside the handler. Use lightweight runtimes (Python, Node.js over Java).

> **Q: What is the difference between synchronous and asynchronous Lambda invocation?**
> **Synchronous:** Caller waits for response — used by API Gateway, ALB. Errors returned directly to caller. **Asynchronous:** Caller doesn't wait — used by S3, SNS, EventBridge. Lambda queues the event and retries up to 2 times on failure. Dead Letter Queue captures failed events.

> **Q: How do you achieve parallel processing in Lambda?**
> Lambda is single-threaded per invocation. For parallelism: trigger multiple concurrent invocations via SQS (multiple messages), Kinesis (multiple shards), or fan-out through SNS → multiple Lambda subscriptions. Within a function, use Python's `concurrent.futures` or `asyncio`.

> **Q: What is the Lambda execution role vs. resource-based policy?**
> **Execution Role (identity-based):** What the Lambda function CAN DO — which AWS services it can call. **Resource-based Policy:** Who/what CAN INVOKE the Lambda function — allows S3, API Gateway, EventBridge to trigger it.

> **Q: How do you pass configuration to Lambda without hardcoding?**
> Environment variables, AWS Systems Manager Parameter Store (`ssm:GetParameter`), AWS Secrets Manager for sensitive values. Never hardcode credentials in function code.

> **Q: What is Lambda@Edge?**
> Lambda functions that run at CloudFront edge locations — triggered by viewer/origin requests and responses. Used for A/B testing, URL rewrites, authentication at the edge, geo-based content personalization.

---

## 9. AWS Glue

### What is AWS Glue?

A **fully managed, serverless ETL (Extract, Transform, Load)** service built on Apache Spark. You write transformation logic, AWS manages the infrastructure.

```mermaid
graph TD
    Glue[AWS Glue] --> Catalog[Glue Data Catalog]
    Glue --> Crawlers[Glue Crawlers]
    Glue --> ETL[Glue ETL Jobs]
    Glue --> Triggers[Triggers & Workflows]
    Glue --> DataBrew[Glue DataBrew]
    Glue --> Studio[Glue Studio - Visual ETL]

    Catalog --> Databases[Databases]
    Databases --> Tables[Tables - Metadata]
    Tables --> Schema[Schema, Location, Format, Partitions]
```

---

### Glue Data Catalog

The **centralized metadata repository** for the entire AWS data ecosystem.

```mermaid
graph TD
    GDC[Glue Data Catalog] --> DB1[Database: raw_db]
    GDC --> DB2[Database: curated_db]
    GDC --> DB3[Database: analytics_db]

    DB1 --> T1[Table: orders - s3://lake/raw/orders/]
    DB1 --> T2[Table: customers - s3://lake/raw/customers/]

    GDC -->|Used by| Athena[Amazon Athena]
    GDC -->|Used by| Redshift[Redshift Spectrum]
    GDC -->|Used by| EMR[Amazon EMR]
    GDC -->|Used by| GlueJobs[Glue ETL Jobs]
```

**Catalog Hierarchy:**
```
AWS Account (one catalog per region)
└── Glue Data Catalog
        └── Database (logical grouping you create)
                └── Table (metadata: schema + S3 location)
                        └── Partition (sub-path in S3)
```

**Catalog vs Database:**
- **Catalog** = The entire metadata service (1 per region per account — auto-exists, you can't create more)
- **Database** = Logical namespace you create inside the catalog to group related tables

---

### Glue Crawlers

Automatically **scan data sources**, infer schemas, and populate the catalog.

```mermaid
sequenceDiagram
    participant You
    participant Crawler
    participant S3
    participant Catalog

    You->>Crawler: Configure (source, IAM role, schedule)
    Crawler->>S3: Scan files (sample data)
    S3-->>Crawler: File contents
    Crawler->>Crawler: Infer schema & format
    Crawler->>Catalog: Create/Update table definition
    Catalog-->>You: Table available for Athena/Glue/EMR
```

**What Crawlers Detect:**
- Column names and data types
- File format (Parquet, CSV, JSON, ORC, Avro)
- Partition structure (`year=2024/month=01/`)
- Schema changes over time (versioning)

**Crawler Data Sources:** S3, RDS, Redshift, DynamoDB, JDBC, MongoDB

---

### Glue ETL Jobs

The core engine — runs **PySpark or Python Shell** transformations serverlessly.

#### Job Types

| Job Type | Engine | Use Case |
|---|---|---|
| **Spark** | Apache Spark | Large-scale data transformation (default) |
| **Python Shell** | Python 3 | Lightweight scripts, Athena queries, small data |
| **Streaming** | Spark Structured Streaming | Real-time processing from Kinesis/Kafka |
| **Ray** | Ray distributed Python | ML preprocessing, Python-native parallelism |

#### DPU — Data Processing Unit

Glue is priced in **DPUs** (Data Processing Units):
- 1 DPU = 4 vCPUs + 16GB RAM
- Minimum 2 DPUs per Spark job
- Billed per DPU-hour (rounded to second)

```mermaid
graph LR
    GlueJob[Glue Spark Job] --> Driver[Driver Node - 1 DPU]
    GlueJob --> Worker1[Worker 1 - 1 DPU]
    GlueJob --> Worker2[Worker 2 - 1 DPU]
    GlueJob --> WorkerN[Worker N - 1 DPU]
```

#### Worker Types

| Worker Type | vCPU | Memory | Best For |
|---|---|---|---|
| **G.1X** | 4 | 16GB | Standard jobs |
| **G.2X** | 8 | 32GB | Memory-intensive, ML |
| **G.4X** | 16 | 64GB | Very large datasets |
| **G.8X** | 32 | 128GB | Extremely large datasets |

---

### Glue ETL — PySpark Example

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from Glue Data Catalog (source: S3)
orders_df = glueContext.create_dynamic_frame.from_catalog(
    database="raw_db",
    table_name="orders"
).toDF()

# Transform
orders_clean = orders_df \
    .filter(col("amount") > 0) \
    .withColumn("order_date", to_date(col("order_date_str"), "yyyy-MM-dd")) \
    .dropDuplicates(["order_id"]) \
    .drop("order_date_str")

# Write to S3 as Parquet partitioned by date
orders_clean.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("s3://my-lake/curated/orders/")

job.commit()
```

---

### Glue Job Bookmarks

**Job Bookmarks** track which data has already been processed — enabling **incremental processing**.

```mermaid
graph LR
    Run1["Job Run 1"] -->|processes| Files1["Files: Jan 1-10"]
    Run1 -->|saves bookmark| BM["Bookmark State"]
    Run2["Job Run 2"] -->|reads bookmark| BM
    Run2 -->|processes only| Files2["Files: Jan 11-20 (new only)"]
```

**Modes:**
- `job-bookmark-enable` → process only new data
- `job-bookmark-disable` → reprocess everything
- `job-bookmark-pause` → suspend bookmark (process all, reset next run)

---

### Glue Workflows

Orchestrate **multiple Glue jobs and crawlers** in sequence or parallel:

```mermaid
graph LR
    Trigger[Scheduled Trigger] --> Crawler[Run Crawler]
    Crawler -->|completed| Job1[ETL Job: Raw → Curated]
    Job1 -->|success| Job2[ETL Job: Curated → Analytics]
    Job1 -->|failure| SNS[SNS Alert]
    Job2 --> Crawler2[Update Catalog Crawler]
```

---

### Glue DataBrew

**Visual, no-code** data preparation tool:
- Point-and-click transformations (filter, rename, type cast, normalize)
- **Recipes** — saved transformation steps, reusable and shareable
- **Profiles** — data quality statistics (null%, distinct count, distribution)
- No PySpark knowledge required
- Outputs to S3 in any format

---

### Glue Schema Evolution

Handling changes in data structure over time:

```python
# Option 1: Merge schemas (addNewColumns)
glueContext.create_dynamic_frame.from_catalog(
    database="raw_db",
    table_name="orders",
    additional_options={"mergeSchema": "true"}  # handles new columns
)

# Option 2: ResolveChoice — handle type conflicts
resolved = ResolveChoice.apply(
    frame=dyf,
    choice="make_struct"  # or "cast:string", "project:type"
)
```

**Schema Evolution Strategies:**
- Add new columns → use `mergeSchema`
- Changed column types → use `ResolveChoice`
- Breaking changes → create new table version in catalog
- Track versions in Glue Data Catalog's table version history

---

### EMR vs Glue

| Feature | EMR | Glue |
|---|---|---|
| **Type** | Managed cluster | Serverless |
| **Frameworks** | Spark, Hadoop, Hive, Flink, Presto | Spark, Python Shell |
| **Control** | Full — instance types, configs, JVM | Limited |
| **Startup time** | 5–10 min | 1–3 min |
| **Cost model** | Per EC2 second + EMR fee | Per DPU-hour |
| **Spot instances** | ✅ Big savings | ❌ |
| **Data volume** | PB-scale | GB-TB scale |
| **Best for** | Complex, large-scale, custom | Standard ETL pipelines |

---

### Glue Interview Questions

> **Q: What is the Glue Data Catalog and why is it important?**
> It's a centralized metadata repository (schema, location, format, partitions) for data assets. It acts as a shared Hive Metastore used by Athena, EMR, Redshift Spectrum, and Glue ETL — define schema once, use everywhere.

> **Q: What is a Glue Crawler and what does it do?**
> A Crawler scans data sources (S3, RDS, Redshift), infers schema and format, detects partitions, and automatically creates/updates table definitions in the Glue Data Catalog.

> **Q: What are Glue Job Bookmarks?**
> Job Bookmarks track which data has been processed, enabling incremental ETL — only processing new files since the last successful run. This avoids reprocessing old data and is essential for cost efficiency.

> **Q: What is the difference between DynamicFrame and DataFrame in Glue?**
> **DynamicFrame** is Glue-native — handles schema inconsistencies and type ambiguity (e.g., a column that's sometimes int, sometimes string). **DataFrame** is standard PySpark — strict schema. DynamicFrames can be converted to DataFrames with `.toDF()` for complex Spark operations.

> **Q: How do you handle schema evolution in Glue ETL jobs?**
> Use `mergeSchema` option for new columns. Use `ResolveChoice` transform for type conflicts. Track schema changes via Glue Catalog table versions. For breaking changes, create new table definitions.

> **Q: How do you pass parameters to a Glue job?**
> Job Parameters are key-value pairs prefixed with `--`. Access via `getResolvedOptions(sys.argv, ['param_name'])`. Can be set at job creation or overridden at runtime (`start_job_run(Arguments={...})`).

> **Q: What is a Glue Workflow?**
> A Glue Workflow orchestrates multiple Glue jobs, crawlers, and triggers in a visual DAG (directed acyclic graph). Supports sequential and parallel execution, conditional branching, and error handling.

---

## 10. Amazon Athena

### What is Athena?

A **serverless, interactive query service** that runs standard SQL directly on data stored in S3. No infrastructure to manage, no data loading required.

```mermaid
graph LR
    Analyst["Analyst / Application"] -->|SQL Query| Athena["Amazon Athena (Presto/Trino)"]
    Athena --> GDC["Glue Data Catalog (Schema)"]
    Athena -->|scan| S3["S3 Data (Parquet/CSV/JSON)"]
    Athena -->|results written to| ResultS3["S3 Results Bucket"]
    ResultS3 --> Analyst
```

---

### Key Properties

| Property | Detail |
|---|---|
| **Engine** | Presto/Trino (open-source SQL) |
| **Pricing** | $5 per TB scanned |
| **Formats** | Parquet, ORC, JSON, CSV, Avro, TSV, Gzip |
| **Metadata** | Glue Data Catalog (or built-in catalog) |
| **Result storage** | S3 bucket (you specify) |
| **Serverless** | Zero infrastructure |
| **Concurrency** | Multiple queries simultaneously |

---

### Athena Query Example

```sql
-- Query partitioned Parquet data — cost-efficient
SELECT
    product_category,
    COUNT(*) AS order_count,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value
FROM curated_db.orders
WHERE year = '2024'          -- partition pruning → only scan 2024 data
  AND month BETWEEN '01' AND '06'
GROUP BY product_category
ORDER BY total_revenue DESC;
```

---

### Cost Optimization

The most critical Athena skill — reduce the data scanned:

```mermaid
graph LR
    CSV["CSV (no compression)
    Scan: 100GB → Cost: $0.50"] -->|Convert| Parquet

    Parquet["Parquet (columnar + Snappy)
    Scan: ~5GB → Cost: $0.025
    Savings: 95%!"] -->|Partition| PartParquet

    PartParquet["Parquet + Partitions
    Query WHERE year=2024 → Scan only 2024 data
    Savings: 95%+ more"]
```

**Best Practices:**
1. **Use Parquet or ORC** — columnar formats, Athena reads only needed columns
2. **Partition data** by common filter columns (`year/month/day`, `region`)
3. **Compress** with Snappy or GZIP
4. **Use larger files** (>128MB) — avoid many small files
5. **Use LIMIT** only for exploration — doesn't reduce scan cost
6. **Use columnar projections** — `SELECT col1, col2` not `SELECT *`

---

### Athena Workgroups

Isolate query execution and **control costs per team**:

```
Workgroup: data-engineering
├── Max data scanned per query: 10GB
├── Results location: s3://results/data-eng/
└── CloudWatch metrics: enabled

Workgroup: analytics-team
├── Max data scanned per query: 5GB
├── Results location: s3://results/analytics/
└── SNS alert if query exceeds 2GB
```

---

### Athena Federated Queries

Query data beyond S3 — connect to RDS, DynamoDB, Redshift, on-prem databases via **Lambda connectors**:

```mermaid
graph LR
    Athena -->|Federated Query| Lambda["Lambda Data Source Connector"]
    Lambda --> RDS[RDS MySQL]
    Lambda --> DynamoDB[DynamoDB]
    Lambda --> Redshift[Redshift]
    Lambda --> OnPrem[On-Premises DB]
    Athena --> S3[S3 Data Lake]
```

Join S3 data with RDS data in a single SQL query!

---

### Athena vs Redshift — When to Use

```
Low frequency, ad-hoc queries?              → Athena
Querying raw/messy S3 files?                → Athena
AWS log analysis (CloudTrail, ALB, VPC)?    → Athena
Serverless, no infra budget?               → Athena
Federated queries across multiple sources? → Athena

High concurrency, BI dashboards?           → Redshift
Complex multi-join analytical queries?      → Redshift
JOIN S3 data with warehouse data?          → Redshift Spectrum
Predictable, sustained heavy workload?     → Redshift
Sub-second response for dashboards?        → Redshift
```

---

### Athena Interview Questions

> **Q: How does Athena reduce query costs?**
> Use columnar formats (Parquet/ORC) to read only necessary columns, partition data so Athena skips irrelevant partitions, compress data to reduce scan size. Together these can reduce costs by 95%+.

> **Q: What is partition pruning in Athena?**
> When you query with `WHERE year=2024 AND month=01`, Athena uses the Glue Data Catalog to identify which S3 paths to scan — skipping all other partitions. Only works if data is properly partitioned and partitions are registered in the catalog.

> **Q: How do you programmatically run Athena queries?**
> Use Boto3 `start_query_execution()` which returns a QueryExecutionId. Then poll `get_query_execution()` to check status. Better approach: use Step Functions Wait state or EventBridge to avoid busy polling.

> **Q: What is Athena CTAS (Create Table As Select)?**
> Creates a new table from query results — useful for materializing expensive queries, converting CSV to Parquet, or repartitioning data.
> ```sql
> CREATE TABLE curated_db.orders_parquet
> WITH (format='PARQUET', partitioned_by=ARRAY['year','month'])
> AS SELECT * FROM raw_db.orders_csv;
> ```

---

## 11. Amazon Redshift

### What is Redshift?

A **fully managed, petabyte-scale cloud data warehouse** optimized for OLAP (Online Analytical Processing) — complex analytical queries across massive datasets.

```mermaid
graph TD
    Client["BI Tool / SQL Client"] -->|SQL| LeaderNode["Leader Node"]
    LeaderNode -->|Query Plan| CN1["Compute Node 1"]
    LeaderNode -->|Query Plan| CN2["Compute Node 2"]
    LeaderNode -->|Query Plan| CN3["Compute Node N"]
    CN1 -->|Results| LeaderNode
    CN2 -->|Results| LeaderNode
    CN3 -->|Results| LeaderNode
    LeaderNode -->|Final Result| Client
```

---

### Columnar Storage

Redshift stores data **column by column** instead of row by row:

```
Row Storage (traditional):
Row 1: [1, John, Electronics, 299.99]
Row 2: [2, Jane, Clothing, 89.99]

Columnar Storage (Redshift):
order_id: [1, 2]
customer: [John, Jane]
category: [Electronics, Clothing]
amount:   [299.99, 89.99]
```

**Benefits:**
- Read only columns needed → less I/O
- Same data type per column → better compression (10–20x)
- Faster aggregations (SUM, COUNT, AVG)

---

### MPP — Massively Parallel Processing

```mermaid
sequenceDiagram
    participant Leader
    participant Slice1
    participant Slice2
    participant Slice3

    Leader->>Slice1: Process rows 1–10M
    Leader->>Slice2: Process rows 10M–20M
    Leader->>Slice3: Process rows 20M–30M
    Slice1-->>Leader: Partial result
    Slice2-->>Leader: Partial result
    Slice3-->>Leader: Partial result
    Leader->>Leader: Aggregate final result
```

---

### Distribution Styles

Controls how data is spread across compute nodes — critical for JOIN performance:

```mermaid
graph TD
    DistStyles[Distribution Styles] --> Auto[AUTO - Redshift decides]
    DistStyles --> Even[EVEN - Round-robin across nodes]
    DistStyles --> Key[KEY - Same distkey value → same node]
    DistStyles --> All[ALL - Full copy on every node]

    Even --> EvenWhen["Use when: no clear join key"]
    Key --> KeyWhen["Use when: large table joins on this key (co-located)"]
    All --> AllWhen["Use when: small dimension tables (always local)"]
```

---

### Sort Keys

Define physical order of data on disk — enables **zone map** block skipping:

```sql
CREATE TABLE orders (
    order_id    INT,
    customer_id INT  DISTKEY,
    order_date  DATE,
    amount      DECIMAL(10,2)
)
SORTKEY(order_date);
-- Queries filtering on order_date skip irrelevant blocks automatically
```

| Sort Key Type | Best For |
|---|---|
| **Compound** (default) | Queries filtering on leading columns of sort key |
| **Interleaved** | Queries filtering equally on multiple columns |

---

### Redshift Spectrum

Extends Redshift to query **S3 data directly** without loading into Redshift:

```mermaid
graph LR
    SQL["SQL Query"] --> Leader[Redshift Leader Node]
    Leader --> Internal["Internal Redshift Tables (hot data)"]
    Leader --> Spectrum["Spectrum Layer"]
    Spectrum --> GDC[Glue Data Catalog]
    Spectrum --> S3["S3 (cold/historical data)"]
    Internal --> Leader
    Spectrum --> Leader
    Leader --> Result[Final Result - JOIN of both]
```

**Key strength:** JOIN hot Redshift data with cold S3 data in a single SQL query.

---

### Loading Data — COPY Command

The fastest, most efficient way to load data:

```sql
-- Load Parquet from S3 into Redshift
COPY orders
FROM 's3://my-data-lake/curated/orders/'
IAM_ROLE 'arn:aws:iam::123456:role/RedshiftS3Role'
FORMAT AS PARQUET;

-- Load CSV from S3
COPY customers
FROM 's3://my-data-lake/raw/customers/'
IAM_ROLE 'arn:aws:iam::123456:role/RedshiftS3Role'
CSV
IGNOREHEADER 1
DELIMITER ','
REGION 'ap-south-1';
```

Loads in **parallel** across all slices — much faster than row-by-row INSERT.

---

### Redshift Performance Features

| Feature | Description |
|---|---|
| **Result Caching** | Identical queries return cached results instantly — 0 compute |
| **Concurrency Scaling** | Burst extra clusters at peak — users never queue |
| **Materialized Views** | Pre-compute expensive aggregations, auto-refresh |
| **VACUUM** | Reclaim space, re-sort rows after deletes/updates |
| **ANALYZE** | Update statistics for better query plans |
| **AQE** | Auto-rewrites queries for better performance |

---

### Redshift Serverless

| | Provisioned | Serverless |
|---|---|---|
| Setup | Choose node type + count | Zero config |
| Scaling | Manual / scheduled | Automatic |
| Cost | Per node-hour | Per RPU-second |
| Best for | Predictable heavy workloads | Intermittent, variable |
| Cold start | None | Slight delay after idle |

---

### Redshift Interview Questions

> **Q: What is the difference between DISTKEY and SORTKEY?**
> **DISTKEY** determines how rows are distributed across compute nodes — affects JOIN performance (co-location). **SORTKEY** determines the physical ordering of rows on disk within each node — affects range filter performance (zone map skipping).

> **Q: What is a zone map in Redshift?**
> Metadata stored per 1MB disk block recording the min and max values. When Redshift executes a range filter query, it checks zone maps to skip entire blocks that can't contain matching rows, dramatically reducing I/O.

> **Q: How do you optimize Redshift query performance?**
> Choose appropriate distribution style (KEY for large table joins, ALL for small dims). Use sort keys on frequently filtered columns. Use VACUUM and ANALYZE regularly. Enable result caching. Use materialized views for complex aggregations. Use WLM (Workload Management) to queue queries by priority.

> **Q: What is Redshift WLM (Workload Management)?**
> WLM defines **query queues** with memory and concurrency allocations. Routes queries based on user group or query group. Ensures ETL jobs don't starve BI dashboard queries and vice versa. Supports auto WLM (AWS manages) and manual WLM.

> **Q: What is the difference between Redshift COPY and INSERT?**
> **COPY** is a bulk-load operation that runs in parallel across all nodes/slices — orders of magnitude faster. **INSERT** is row-by-row and extremely slow for large datasets. Always use COPY for bulk loading.

> **Q: How does Redshift Spectrum differ from Athena?**
> Spectrum is a **feature of Redshift** — requires a running Redshift cluster. Its key strength is joining Redshift internal tables with S3 external tables in one query. Athena is standalone and serverless — no Redshift needed, but cannot join with Redshift tables.

---

## 12. Amazon SNS

### What is SNS?

**Simple Notification Service** — fully managed **pub/sub (publish-subscribe)** messaging. One publisher → many subscribers simultaneously.

```mermaid
graph TD
    Publisher["Publisher (S3 Event / App / CloudWatch)"] -->|Publish| Topic["SNS Topic"]
    Topic -->|push| Email[Email / Email-JSON]
    Topic -->|push| SMS[SMS]
    Topic -->|push| Lambda[AWS Lambda]
    Topic -->|push| SQS[Amazon SQS Queue]
    Topic -->|push| HTTP[HTTP/HTTPS Endpoint]
    Topic -->|push| Mobile[Mobile Push - APNs/FCM]
    Topic -->|stream| Kinesis[Kinesis Firehose]
```

---

### Core Concepts

| Concept | Description |
|---|---|
| **Topic** | Communication channel — publishers write to it, subscribers listen |
| **Publisher** | Any AWS service or app sending a message |
| **Subscriber** | Endpoint receiving messages (must subscribe + confirm) |
| **Message** | JSON payload up to 256KB |

---

### Message Filtering

By default every subscriber gets every message. **Filter policies** route selectively:

```json
// Subscriber A filter: only electronics orders
{
  "orderType": ["electronics"]
}

// Subscriber B filter: orders over $500
{
  "amount": [{"numeric": [">", 500]}]
}
```

---

### SNS + SQS Fan-Out Pattern

The most common AWS architecture pattern for data pipelines:

```mermaid
graph TD
    S3Event["S3 Event: New File"] -->|trigger| SNS["SNS Topic: new-data-arrived"]
    SNS -->|fan-out| SQS1["SQS Queue A → Lambda: Process & Transform"]
    SNS -->|fan-out| SQS2["SQS Queue B → Lambda: Send Notification"]
    SNS -->|fan-out| SQS3["SQS Queue C → Lambda: Update Catalog"]
```

**Why fan-out?**
- All queues receive message **simultaneously**
- Each queue processes **independently** at its own pace
- If one consumer fails, others are **unaffected**
- SQS adds **durability** — messages persisted until processed

---

### SNS Standard vs FIFO

| Feature | Standard Topic | FIFO Topic |
|---|---|---|
| Ordering | ❌ Best-effort | ✅ Strict |
| Deduplication | ❌ | ✅ |
| Throughput | Very high | 300 msg/sec |
| Subscribers | All types | SQS FIFO only |

---

### SNS Interview Questions

> **Q: What is the difference between SNS and SQS?**
> **SNS** is push-based pub/sub — one message broadcast to many subscribers simultaneously, no persistence. **SQS** is a pull-based queue — messages stored until a consumer polls and processes them, one consumer group. They complement each other: SNS fans out, SQS buffers.

> **Q: What happens if an SNS subscriber is unavailable?**
> SNS does NOT persist messages. If a subscriber (HTTP endpoint) is unavailable, the message is lost. This is why SNS → SQS is the recommended pattern — SQS stores the message until the consumer is ready.

> **Q: What is the SNS + SQS fan-out pattern and why is it used?**
> One SNS topic fans out to multiple SQS queues, each with its own consumer. This enables multiple independent services to react to the same event without being coupled. If one consumer fails, others are unaffected. Provides durability (SQS) + broadcast (SNS).

---

## 13. Amazon CloudWatch

### What is CloudWatch?

AWS's **fully managed observability service** — collects, monitors, analyzes, and acts on metrics, logs, and events from your AWS infrastructure.

```mermaid
graph TD
    CW[Amazon CloudWatch] --> Metrics[Metrics - Time-series numbers]
    CW --> Logs[Logs - Text log data]
    CW --> Alarms[Alarms - Threshold-based alerts]
    CW --> Dashboards[Dashboards - Visual monitoring]
    CW --> Insights[Logs Insights - SQL-like log queries]
    CW --> Anomaly[Anomaly Detection - ML-based]
    CW --> Events[CloudWatch Events - now EventBridge]
```

---

### CloudWatch Metrics

Numerical time-series data automatically collected from AWS services:

| Service | Key Metrics |
|---|---|
| **EC2** | CPUUtilization, NetworkIn/Out, DiskReadOps |
| **Lambda** | Invocations, Errors, Duration, Throttles, ConcurrentExecutions |
| **Glue** | jvm.heap.usage, s3.filesystem.read_bytes, s3.filesystem.write_bytes |
| **RDS** | DatabaseConnections, FreeStorageSpace, ReadLatency |
| **SQS** | NumberOfMessagesSent, ApproximateAgeOfOldestMessage |
| **Redshift** | CPUUtilization, PercentageDiskSpaceUsed, DatabaseConnections |

**Custom Metrics via Boto3:**

```python
import boto3

cloudwatch = boto3.client('cloudwatch')

cloudwatch.put_metric_data(
    Namespace='DataPipeline/Orders',
    MetricData=[{
        'MetricName': 'RecordsProcessed',
        'Value': 150000,
        'Unit': 'Count',
        'Dimensions': [
            {'Name': 'Environment', 'Value': 'production'},
            {'Name': 'Pipeline', 'Value': 'orders-etl'}
        ]
    }]
)
```

---

### CloudWatch Alarms

```mermaid
stateDiagram-v2
    [*] --> INSUFFICIENT_DATA: No data yet
    INSUFFICIENT_DATA --> OK: Metric within threshold
    OK --> ALARM: Threshold breached for N periods
    ALARM --> OK: Metric returns to normal
    ALARM --> INSUFFICIENT_DATA: Data stops

    ALARM --> Actions: Trigger Actions
    Actions --> SNS: Send email/SMS
    Actions --> AutoScaling: Scale in/out
    Actions --> Lambda: Remediation
    Actions --> EC2: Stop/Start/Reboot
```

**Composite Alarms:** Combine multiple alarms with AND/OR logic to reduce false positives.

---

### CloudWatch Logs

```
Log Group: /aws/glue/jobs/orders-etl-job
├── Log Stream: jr_abc123456 (Job Run 1)
│       ├── 2024-01-15 10:00:01  INFO  Job started
│       ├── 2024-01-15 10:00:15  INFO  Read 1.2M rows from S3
│       ├── 2024-01-15 10:01:30  INFO  Transformation complete
│       └── 2024-01-15 10:01:45  INFO  Written 1.2M rows to S3
└── Log Stream: jr_def789 (Job Run 2)
        ├── 2024-01-16 11:00:01  INFO  Job started
        └── 2024-01-16 11:00:30  ERROR Failed: S3 Access Denied
```

**Automatic log sources:** Lambda, Glue, ECS, CloudTrail, RDS
**Requires CloudWatch Agent:** EC2 (for memory metrics, custom logs)

---

### CloudWatch Logs Insights

SQL-like query language for log data:

```
# Find all errors in Glue jobs (last 24 hours)
fields @timestamp, @message
| filter @message like /ERROR/
| sort @timestamp desc
| limit 100

# Lambda duration analysis
fields @timestamp, @duration, @billedDuration
| stats
    avg(@duration) as avg_duration,
    max(@duration) as max_duration,
    percentile(@duration, 95) as p95_duration
  by bin(5m)

# Count errors by type
parse @message "* ERROR: *" as timestamp, errorMsg
| stats count(*) as errorCount by errorMsg
| sort errorCount desc
```

---

### CloudWatch vs CloudTrail

| | CloudWatch | CloudTrail |
|---|---|---|
| **Purpose** | Monitor performance & health | Audit API calls & actions |
| **Tracks** | Metrics, logs, events | Who did what, when, from where |
| **Example** | CPU at 90%, Lambda errored | User X deleted S3 bucket at 3PM |
| **Use case** | Operations, alerting | Security, compliance, audit |

---

### CloudWatch for Data Engineering

```
Glue Job Monitoring:
├── Alarm: Glue job failed → SNS → PagerDuty
├── Dashboard: Job duration trend, bytes processed
└── Logs: /aws/glue/jobs/* for debugging

Lambda Monitoring:
├── Alarm: Error rate > 1% → SNS alert
├── Alarm: Duration > 10min → check for infinite loops
└── Metrics: Throttle count (need reserved concurrency)

Redshift Monitoring:
├── Alarm: DiskSpaceUsed > 80% → SNS → add storage
├── Metric: QueryDuration → identify slow queries
└── Logs: User activity log → audit queries
```

---

### CloudWatch Interview Questions

> **Q: What is the difference between CloudWatch and CloudTrail?**
> CloudWatch monitors operational health — metrics (CPU, memory, request count), logs, and alarms. CloudTrail audits AWS API activity — who made what API call, from where, at what time. CloudWatch answers "Is my system healthy?" CloudTrail answers "Who did what to my system?"

> **Q: How do you monitor a Glue ETL job with CloudWatch?**
> Glue automatically sends logs to `/aws/glue/jobs/` log groups. Monitor built-in Glue metrics (DPU usage, bytes read/written). Set CloudWatch Alarms on the `glue.driver.aggregate.numFailedTasks` metric or create an alarm triggered by job state change via EventBridge.

> **Q: What is a CloudWatch Alarm's INSUFFICIENT_DATA state?**
> The alarm doesn't have enough data to evaluate whether the threshold has been breached — occurs at startup or if metric data stops flowing. Should be treated similarly to ALARM in production monitoring.

---

## 14. AWS Step Functions

### What is Step Functions?

A **serverless workflow orchestration** service that coordinates multiple AWS services into reliable, visual state machines (workflows).

```mermaid
graph TD
    SF[AWS Step Functions] -->|defines| StateMachine[State Machine - Visual Workflow]
    StateMachine -->|written in| ASL[Amazon States Language - JSON/YAML]
    StateMachine -->|coordinates| Lambda[Lambda Functions]
    StateMachine -->|coordinates| Glue[Glue Jobs]
    StateMachine -->|coordinates| ECS[ECS Tasks]
    StateMachine -->|coordinates| Athena[Athena Queries]
    StateMachine -->|coordinates| SNS[SNS Notifications]
    StateMachine -->|coordinates| SQS[SQS Queues]
```

---

### State Types

| State Type | Description |
|---|---|
| **Task** | Run a unit of work (Lambda, Glue, ECS, etc.) |
| **Choice** | Conditional branching (if/else) |
| **Wait** | Pause for a fixed time or until a timestamp |
| **Parallel** | Run multiple branches simultaneously |
| **Map** | Iterate over an array, process each item |
| **Pass** | Pass input to output (no work done) |
| **Succeed** | End execution successfully |
| **Fail** | End execution with failure |

---

### Step Functions Workflow Example

A data pipeline workflow:

```mermaid
graph TD
    Start([Start]) --> Validate["Task: Lambda - Validate File Schema"]
    Validate --> IsValid{Choice: Is Valid?}
    IsValid -->|Yes| RunGlue["Task: Glue Job - Transform Data"]
    IsValid -->|No| AlertFail["Task: SNS - Alert Bad File"]
    AlertFail --> Fail([Fail])
    RunGlue --> WaitCheck["Wait: 30 seconds"]
    WaitCheck --> CheckStatus["Task: Lambda - Check Glue Status"]
    CheckStatus --> GlueDone{Choice: Glue Done?}
    GlueDone -->|Running| WaitCheck
    GlueDone -->|Success| Parallel["Parallel: Run 3 tasks simultaneously"]
    GlueDone -->|Failed| AlertGlueFail["Task: SNS - Alert Glue Failure"]
    Parallel --> UpdateCatalog["Task: Lambda - Update Glue Catalog"]
    Parallel --> RunQuality["Task: Lambda - Data Quality Check"]
    Parallel --> NotifyTeam["Task: SNS - Notify Team"]
    UpdateCatalog --> Success([Succeed])
    RunQuality --> Success
    NotifyTeam --> Success
```

---

### Amazon States Language (ASL)

```json
{
  "Comment": "Data Pipeline Workflow",
  "StartAt": "ValidateFile",
  "States": {
    "ValidateFile": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:123:function:validate-file",
      "Next": "CheckValidation",
      "Retry": [{
        "ErrorEquals": ["Lambda.ServiceException"],
        "IntervalSeconds": 2,
        "MaxAttempts": 3,
        "BackoffRate": 2
      }],
      "Catch": [{
        "ErrorEquals": ["States.ALL"],
        "Next": "AlertFailure"
      }]
    },
    "CheckValidation": {
      "Type": "Choice",
      "Choices": [{
        "Variable": "$.isValid",
        "BooleanEquals": true,
        "Next": "RunGlueJob"
      }],
      "Default": "AlertFailure"
    },
    "RunGlueJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync:2",
      "Parameters": {
        "JobName": "orders-etl-job",
        "Arguments": {
          "--input_path.$": "$.s3_path"
        }
      },
      "Next": "NotifySuccess"
    },
    "NotifySuccess": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:us-east-1:123:pipeline-alerts",
        "Message": "Pipeline completed successfully!"
      },
      "End": true
    },
    "AlertFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:us-east-1:123:pipeline-alerts",
        "Message": "Pipeline FAILED!"
      },
      "End": true
    }
  }
}
```

---

### Standard vs Express Workflows

| Feature | Standard | Express |
|---|---|---|
| **Duration** | Up to 1 year | Up to 5 minutes |
| **Execution rate** | 2,000/sec | 100,000/sec |
| **Pricing** | Per state transition | Per execution duration |
| **Auditing** | Full history in console | CloudWatch Logs |
| **Use case** | Long-running pipelines, human approval | High-volume, short workflows |

---

### Error Handling in Step Functions

```json
"Retry": [{
  "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException"],
  "IntervalSeconds": 2,
  "MaxAttempts": 3,
  "BackoffRate": 2.0
}],
"Catch": [{
  "ErrorEquals": ["States.ALL"],
  "Next": "HandleError",
  "ResultPath": "$.error"
}]
```

**Retry:** Automatically retry on specific errors with exponential backoff.
**Catch:** Route to a fallback state when retries are exhausted.

---

### Step Functions vs Glue Workflows vs Airflow

| Feature | Step Functions | Glue Workflows | Airflow |
|---|---|---|---|
| **Scope** | Any AWS service | Glue only | Any |
| **Serverless** | ✅ | ✅ | ❌ (self-managed or MWAA) |
| **Error handling** | ✅ Built-in retry/catch | Basic | ✅ Extensive |
| **Visualization** | ✅ Real-time visual | Basic | ✅ |
| **Cross-service** | ✅ 200+ AWS services | ❌ Glue only | ✅ |
| **Best for** | Complex AWS orchestration | Glue-only pipelines | General data pipelines |

---

### Step Functions Interview Questions

> **Q: What is AWS Step Functions and why is it used in data engineering?**
> Step Functions is a serverless workflow orchestrator that coordinates multiple AWS services into reliable state machines. In data engineering, it orchestrates ETL pipelines, handles retries, conditional branching, parallel processing, and error handling — replacing brittle custom glue code.

> **Q: What is the difference between Standard and Express Workflows?**
> Standard Workflows support executions up to 1 year, have full execution history in the console, and are priced per state transition — ideal for long-running data pipelines. Express Workflows support up to 5 minutes, much higher throughput (100K/sec), and are priced per duration — ideal for high-volume, short event-driven workflows.

> **Q: How does Step Functions handle errors?**
> Through **Retry** (retry the same state with configurable backoff) and **Catch** (route to a different state when retries fail). You can specify which error types trigger each, with exponential backoff to avoid thundering herd problems.

> **Q: What is the Map state in Step Functions?**
> The Map state processes each element of an input array in parallel — like a distributed forEach. Useful for processing multiple S3 files, validating multiple records, or running the same transformation on multiple datasets simultaneously.

---

## 15. Amazon EventBridge

### What is EventBridge?

A **serverless event bus** that routes events from sources to targets based on rules. The backbone of event-driven architectures on AWS.

```mermaid
graph TD
    Sources["Event Sources"] --> Bus["EventBridge Event Bus"]
    Bus -->|Rule matches| Target1["Lambda: Process file"]
    Bus -->|Rule matches| Target2["Glue: Run ETL"]
    Bus -->|Rule matches| Target3["SNS: Alert team"]
    Bus -->|Rule matches| Target4["Step Functions: Start workflow"]

    subgraph Sources
        AWSServices["AWS Services (S3, EC2, RDS...)"]
        CustomApp["Custom App Events"]
        SaaS["SaaS Partners (Salesforce, Zendesk)"]
    end
```

---

### Key Components

| Component | Description |
|---|---|
| **Event** | JSON document describing something that happened |
| **Event Bus** | Channel through which events flow |
| **Rule** | Filter + router — matches events and sends to targets |
| **Target** | Destination for matched events (Lambda, Glue, SQS, etc.) |
| **Schedule** | Cron/rate expressions for time-based triggers |

---

### Three Bus Types

| Bus | Source | Use Case |
|---|---|---|
| **Default** | All AWS services automatically | Monitor AWS service events |
| **Custom** | Your applications | Decouple microservices |
| **Partner** | SaaS (Salesforce, Datadog, GitHub) | SaaS event integration |

---

### Scheduled Rules (Cron Jobs)

```
# Every day at 2 AM UTC
cron(0 2 * * ? *)

# Every Monday at 9 AM
cron(0 9 ? * MON *)

# Every 5 minutes
rate(5 minutes)

# First of every month
cron(0 0 1 * ? *)
```

---

### EventBridge in Data Pipelines

```mermaid
graph LR
    S3["New S3 File"] -->|S3 Event| EB[EventBridge]
    EB --> Lambda1["Lambda: Validate"]
    Lambda1 -->|Custom Event: Validated| EB
    EB --> SF["Step Functions: Full Pipeline"]
    SF -->|Job Complete Event| EB
    EB --> SNS["SNS: Notify team"]
```

---

### EventBridge vs SNS vs SQS

| | EventBridge | SNS | SQS |
|---|---|---|---|
| **Model** | Event routing | Pub/Sub | Queue |
| **Filtering** | ✅ Rich JSON patterns | ✅ Basic | ❌ |
| **Scheduling** | ✅ Cron/rate | ❌ | ❌ |
| **SaaS partners** | ✅ | ❌ | ❌ |
| **Archive & Replay** | ✅ | ❌ | ❌ |
| **Latency** | ~500ms | Milliseconds | Milliseconds |

---

## 16. Boto3 — AWS SDK for Python

### What is Boto3?

The **official AWS SDK for Python** — programmatically interact with any AWS service from Python code.

```mermaid
graph LR
    PythonCode["Python Code"] -->|boto3| BotoCore["Botocore - Core HTTP/Auth"]
    BotoCore -->|HTTPS API calls| AWS["AWS Services"]
    AWS -->|JSON responses| PythonCode
```

---

### Client vs Resource

```python
import boto3

# CLIENT: Low-level API — maps 1:1 with AWS service APIs
# Returns JSON/dict responses
s3_client = boto3.client('s3', region_name='ap-south-1')

# RESOURCE: High-level, object-oriented API
# Returns Python objects with attributes and methods
s3_resource = boto3.resource('s3', region_name='ap-south-1')

# Client style:
response = s3_client.get_object(Bucket='my-bucket', Key='file.csv')
data = response['Body'].read()

# Resource style:
obj = s3_resource.Object('my-bucket', 'file.csv')
data = obj.get()['Body'].read()
```

| | client | resource |
|---|---|---|
| Level | Low-level | High-level OO |
| Returns | dict/JSON | Python objects |
| Coverage | All services | Select services |
| Use when | Full API control | Simpler CRUD operations |

---

### Authentication & Sessions

```python
import boto3

# Option 1: Default credential chain (recommended for AWS services)
# Checks: env vars → ~/.aws/credentials → IAM role (EC2/Lambda/ECS)
s3 = boto3.client('s3')

# Option 2: Named profile (for local development)
session = boto3.Session(profile_name='dev-profile')
s3 = session.client('s3')

# Option 3: Explicit credentials (avoid in production!)
s3 = boto3.client(
    's3',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
)

# Option 4: Assume a role (cross-account)
sts = boto3.client('sts')
assumed = sts.assume_role(
    RoleArn='arn:aws:iam::999999:role/CrossAccountRole',
    RoleSessionName='my-session'
)
creds = assumed['Credentials']
s3 = boto3.client(
    's3',
    aws_access_key_id=creds['AccessKeyId'],
    aws_secret_access_key=creds['SecretAccessKey'],
    aws_session_token=creds['SessionToken']
)
```

---

### Common Boto3 Patterns for Data Engineers

#### S3 Operations

```python
import boto3
import pandas as pd
from io import BytesIO

s3 = boto3.client('s3')

# Upload file
s3.upload_file('local_file.parquet', 'my-bucket', 'data/file.parquet')

# Download file
s3.download_file('my-bucket', 'data/file.parquet', 'local_copy.parquet')

# Read Parquet directly into pandas (no download)
obj = s3.get_object(Bucket='my-bucket', Key='data/orders.parquet')
df = pd.read_parquet(BytesIO(obj['Body'].read()))

# List objects with prefix
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket='my-bucket', Prefix='raw/2024/'):
    for obj in page.get('Contents', []):
        print(obj['Key'])

# Copy object between buckets
s3.copy_object(
    CopySource={'Bucket': 'source-bucket', 'Key': 'data/file.parquet'},
    Bucket='dest-bucket',
    Key='data/file.parquet'
)

# Delete object
s3.delete_object(Bucket='my-bucket', Key='data/old_file.csv')

# Generate presigned URL
url = s3.generate_presigned_url(
    'get_object',
    Params={'Bucket': 'my-bucket', 'Key': 'report.pdf'},
    ExpiresIn=3600  # 1 hour
)
```

#### Glue Operations

```python
glue = boto3.client('glue')

# Start a Glue job
response = glue.start_job_run(
    JobName='orders-etl-job',
    Arguments={
        '--input_date': '2024-01-15',
        '--output_bucket': 'my-data-lake'
    }
)
job_run_id = response['JobRunId']

# Check job status
status = glue.get_job_run(
    JobName='orders-etl-job',
    RunId=job_run_id
)
print(status['JobRun']['JobRunState'])  # RUNNING, SUCCEEDED, FAILED

# Get table from Data Catalog
table = glue.get_table(
    DatabaseName='curated_db',
    Name='orders'
)

# Create partition in catalog
glue.create_partition(
    DatabaseName='curated_db',
    TableName='orders',
    PartitionInput={
        'Values': ['2024', '01'],
        'StorageDescriptor': {
            'Location': 's3://my-lake/curated/orders/year=2024/month=01/',
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            }
        }
    }
)
```

#### Athena Operations

```python
import time

athena = boto3.client('athena')

# Execute query
response = athena.start_query_execution(
    QueryString="""
        SELECT product_category, SUM(amount) as revenue
        FROM curated_db.orders
        WHERE year = '2024'
        GROUP BY product_category
    """,
    QueryExecutionContext={'Database': 'curated_db'},
    ResultConfiguration={
        'OutputLocation': 's3://my-bucket/athena-results/'
    }
)
query_execution_id = response['QueryExecutionId']

# Wait for completion
def wait_for_query(query_id):
    while True:
        response = athena.get_query_execution(QueryExecutionId=query_id)
        state = response['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return state
        time.sleep(2)

state = wait_for_query(query_execution_id)

# Get results
if state == 'SUCCEEDED':
    results = athena.get_query_results(QueryExecutionId=query_execution_id)
    for row in results['ResultSet']['Rows'][1:]:  # skip header
        print([col.get('VarCharValue', '') for col in row['Data']])
```

#### Lambda Operations

```python
import json

lambda_client = boto3.client('lambda')

# Invoke Lambda synchronously
response = lambda_client.invoke(
    FunctionName='my-data-validator',
    InvocationType='RequestResponse',  # synchronous
    Payload=json.dumps({'bucket': 'my-bucket', 'key': 'raw/data.csv'})
)
result = json.loads(response['Payload'].read())

# Invoke Lambda asynchronously (fire and forget)
lambda_client.invoke(
    FunctionName='my-notification-sender',
    InvocationType='Event',  # asynchronous
    Payload=json.dumps({'message': 'Pipeline complete'})
)
```

#### Step Functions Operations

```python
sf = boto3.client('stepfunctions')

# Start execution
response = sf.start_execution(
    stateMachineArn='arn:aws:states:ap-south-1:123:stateMachine:DataPipeline',
    name='pipeline-run-2024-01-15',
    input=json.dumps({
        's3_bucket': 'my-data-lake',
        's3_key': 'raw/orders/2024/01/15/orders.parquet',
        'execution_date': '2024-01-15'
    })
)
execution_arn = response['executionArn']

# Check execution status
status = sf.describe_execution(executionArn=execution_arn)
print(status['status'])  # RUNNING, SUCCEEDED, FAILED, ABORTED
```

#### CloudWatch Operations

```python
cw = boto3.client('cloudwatch')

# Publish custom metric
cw.put_metric_data(
    Namespace='DataPipeline/ETL',
    MetricData=[{
        'MetricName': 'RecordsProcessed',
        'Value': 1_500_000,
        'Unit': 'Count',
        'Dimensions': [
            {'Name': 'JobName', 'Value': 'orders-etl'},
            {'Name': 'Environment', 'Value': 'prod'}
        ]
    }]
)

# Create an alarm
cw.put_metric_alarm(
    AlarmName='glue-job-failed',
    ComparisonOperator='GreaterThanThreshold',
    EvaluationPeriods=1,
    MetricName='glue.driver.aggregate.numFailedTasks',
    Namespace='Glue',
    Period=300,
    Statistic='Sum',
    Threshold=0,
    ActionsEnabled=True,
    AlarmActions=['arn:aws:sns:ap-south-1:123:alert-topic'],
    TreatMissingData='notBreaching'
)
```

---

### Paginators — Handling Large Results

AWS APIs return paginated results. Always use paginators for large datasets:

```python
# WRONG — only gets first page
response = s3.list_objects_v2(Bucket='my-bucket', Prefix='raw/')
objects = response['Contents']  # may miss thousands of objects!

# CORRECT — paginate through all results
paginator = s3.get_paginator('list_objects_v2')
all_objects = []
for page in paginator.paginate(Bucket='my-bucket', Prefix='raw/'):
    all_objects.extend(page.get('Contents', []))
```

---

### Boto3 Error Handling

```python
from botocore.exceptions import ClientError, NoCredentialsError

try:
    s3.get_object(Bucket='my-bucket', Key='file.parquet')
except ClientError as e:
    error_code = e.response['Error']['Code']
    if error_code == 'NoSuchKey':
        print("File not found")
    elif error_code == 'AccessDenied':
        print("No permission")
    elif error_code == 'NoSuchBucket':
        print("Bucket does not exist")
    else:
        raise
except NoCredentialsError:
    print("AWS credentials not configured")
```

---

### Boto3 Interview Questions

> **Q: What is the difference between boto3.client() and boto3.resource()?**
> `client` provides a low-level API with 1:1 mapping to AWS service APIs, returns JSON/dict, covers all services. `resource` provides a high-level object-oriented API, returns Python objects, covers select services. Use `client` for full control, `resource` for simpler CRUD operations.

> **Q: How does Boto3 authenticate?**
> Boto3 follows the credential chain: (1) Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), (2) `~/.aws/credentials` file, (3) AWS Config file, (4) IAM Instance Profile (EC2), (5) IAM Task Role (ECS/Fargate), (6) IAM Execution Role (Lambda). AWS services (Lambda, Glue, EC2) automatically use their attached IAM roles.

> **Q: What is a Paginator in Boto3 and why is it important?**
> AWS APIs return paginated results with a NextToken when there are more items. Paginators automatically handle pagination, iterating through all pages. Without them, you only get the first page and silently miss data — critical bug in production pipelines listing S3 objects or Glue tables.

> **Q: How do you trigger a Glue job from Lambda using Boto3?**
> ```python
> glue = boto3.client('glue')
> response = glue.start_job_run(JobName='my-etl-job', Arguments={'--date': '2024-01-15'})
> ```
> The Lambda function's IAM execution role must have `glue:StartJobRun` permission.

---

## 17. Data Lake Architecture on AWS

### Reference Architecture

```mermaid
graph TD
    Sources["Data Sources"] --> Ingest["Ingestion Layer"]
    Ingest --> Raw["Raw Zone - S3"]
    Raw --> Process["Processing Layer"]
    Process --> Curated["Curated Zone - S3 Parquet+Partitioned"]
    Curated --> Analytics["Analytics Layer"]
    Analytics --> Consume["Consumption Layer"]

    subgraph Sources
        RDS["RDS/Aurora"]
        Apps["Applications"]
        Streams["Kafka/Kinesis"]
        Files["CSV/JSON Files"]
    end

    subgraph Ingest
        DMS["AWS DMS - CDC"]
        Lambda1["Lambda - File processing"]
        Firehose["Kinesis Firehose"]
        Glue1["Glue Jobs - Batch"]
    end

    subgraph Process
        GlueETL["Glue ETL - Transform"]
        EMR["EMR - Heavy Spark"]
        SF["Step Functions - Orchestrate"]
    end

    subgraph Analytics
        Athena["Athena - Ad-hoc SQL"]
        Redshift["Redshift - Warehouse"]
        Spectrum["Redshift Spectrum"]
    end

    subgraph Consume
        BI["QuickSight/Tableau/PowerBI"]
        Notebooks["SageMaker Notebooks"]
        APIs["Data APIs"]
    end

    GDC["Glue Data Catalog"] --> Athena
    GDC --> Redshift
    GDC --> GlueETL
    GDC --> EMR

    CW["CloudWatch"] --> SF
    EventBridge["EventBridge"] --> SF
```

### Medallion Architecture on AWS

```
Bronze Layer (Raw Zone)       → S3: raw data as-is, immutable
Silver Layer (Curated Zone)   → S3: cleaned, deduplicated Parquet
Gold Layer (Analytics Zone)   → S3 or Redshift: business-ready aggregations
```

---

## 18. Most Asked Interview Questions

### General AWS Data Engineering

> **Q: How would you design a data lake on AWS?**
> Use S3 as storage (Bronze/Silver/Gold zones), Glue for ETL and Data Catalog, Athena for ad-hoc SQL, Redshift for heavy BI workloads, EventBridge + Step Functions for orchestration, IAM + Lake Formation for security, CloudWatch for monitoring.

> **Q: How do you build a real-time data pipeline on AWS?**
> Kinesis Data Streams → Kinesis Firehose → S3 (buffered) or Kinesis Analytics (real-time SQL). Lambda as a processor. EventBridge for event routing. Step Functions for complex orchestration.

> **Q: What is the difference between AWS Glue and AWS Lambda for data processing?**
> **Glue** is optimized for large-scale ETL — runs Spark, handles TB-scale data, has built-in catalog integration, job bookmarks. **Lambda** is for lightweight, event-driven processing — quick file validation, small transformations, triggering other services. Max 15 min, 10GB memory. Use Glue for ETL, Lambda for orchestration/lightweight tasks.

> **Q: How do you ensure data quality in an AWS pipeline?**
> AWS Glue DataBrew for visual profiling. Custom validation Lambda functions post-ingestion. AWS Deequ (open-source on Glue/EMR) for data quality checks. Store quality metrics in CloudWatch. Quarantine bad records to an error prefix in S3. Alert via SNS on quality failures.

> **Q: How do you handle schema evolution in a data lake?**
> Use Parquet/ORC (schema stored in file). Register schema in Glue Data Catalog and track versions. Use `mergeSchema` in Glue/Spark for additive changes. Use Glue ResolveChoice for type conflicts. For breaking changes, version your table names (`orders_v2`).

> **Q: What is the difference between SQS and Kinesis?**
> **SQS:** Message queue — each message consumed by one consumer, retention up to 14 days, no ordering guarantee (Standard) or strict ordering (FIFO). **Kinesis Data Streams:** Data stream — multiple consumers can read the same record, ordered within a shard, retention up to 365 days. Use SQS for task queues; Kinesis for real-time streaming pipelines with multiple consumers.

> **Q: How do you handle data deduplication in a Glue job?**
> Use PySpark's `dropDuplicates(['id_column'])` on the DataFrame. For streaming, use Kinesis's sequence numbers or DynamoDB as a seen-IDs store. Leverage Delta Lake or Iceberg MERGE operations for upserts.

> **Q: What is AWS Lake Formation?**
> A service that builds on Glue Data Catalog to add fine-grained data access control — column-level, row-level, and table-level permissions across Athena, Redshift Spectrum, Glue, and EMR. Centralizes data lake security instead of managing IAM policies per service.

---

### Architecture Design Questions

> **Q: How do you trigger a Glue job automatically when a file lands in S3?**

```mermaid
graph LR
    S3["File lands in S3"] -->|Event Notification| EventBridge["EventBridge Rule"]
    EventBridge -->|Pattern: Object Created| Lambda["Lambda: Trigger Glue"]
    Lambda -->|start_job_run| Glue["Glue ETL Job"]
    Glue -->|Job State Event| EventBridge2["EventBridge: Glue Succeeded"]
    EventBridge2 --> SNS["SNS: Notify team"]
```

> **Q: How do you run Athena queries sequentially without overlapping?**
> Use Step Functions with Task state (Athena StartQueryExecution) + Wait state + Lambda polling `get_query_execution` status. Or use Athena's native Step Functions integration with `.sync` which waits for completion automatically.

> **Q: How would you optimize a slow Athena query?**
> Convert data to Parquet/ORC, add partitions on filter columns, run `MSCK REPAIR TABLE` to add partitions to catalog, use `SELECT` specific columns instead of `*`, check if data is stored in large files (>128MB), use Athena Workgroups to limit scan per query for cost awareness.

---

### Quick Reference — Service Cheat Sheet

| Service | Type | Key Use Case |
|---|---|---|
| **S3** | Object Storage | Data lake storage, backups, static files |
| **IAM** | Security | Identity, access control, roles |
| **EC2** | Compute | Virtual machines, EMR nodes |
| **ECR** | Container Registry | Store Docker images for Lambda/ECS/EKS |
| **Lambda** | Serverless Compute | Event-driven processing, pipeline triggers |
| **Glue** | Serverless ETL | ETL jobs, Data Catalog, Crawlers |
| **Athena** | Serverless SQL | Ad-hoc SQL on S3 |
| **Redshift** | Data Warehouse | BI, complex analytical queries |
| **SNS** | Pub/Sub Messaging | Fan-out notifications, alerts |
| **SQS** | Message Queue | Async processing, decoupling |
| **EventBridge** | Event Bus | Event routing, cron scheduling |
| **CloudWatch** | Observability | Metrics, logs, alarms, dashboards |
| **Step Functions** | Orchestration | Workflow automation, pipeline coordination |
| **Boto3** | Python SDK | Programmatic AWS control from Python |
| **EMR** | Managed Cluster | Large-scale Spark/Hadoop |
| **Lake Formation** | Data Lake Security | Fine-grained column/row access |
| **Kinesis** | Streaming | Real-time data ingestion and processing |

---

*📘 Notes compiled for Data Engineering interview preparation.*
*Last updated: June 2026*
