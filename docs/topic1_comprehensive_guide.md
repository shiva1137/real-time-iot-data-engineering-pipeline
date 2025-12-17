# Topic 1: Project Setup & Architecture - Comprehensive Guide

## 📋 Table of Contents
1. [Real-World Data Engineering Challenges](#real-world-data-engineering-challenges)
2. [WHY, WHAT, HOW TO, WHAT IF - Comprehensive Explanations](#why-what-how-to-what-if)
3. [Interview Preparation - Comprehensive Q&A](#interview-preparation)
4. [Learning-by-Doing Exercises](#learning-by-doing-exercises)
5. [Modern Data Engineering Practices](#modern-data-engineering-practices)

---

## 📊 REAL-WORLD DATA ENGINEERING CHALLENGES

### Challenge 1: Inconsistent Development Environments

**Problem**: 
Developers have different Python versions, package versions, and system configurations, causing "works on my machine" issues.

**Why It Happens**: 
- Different operating systems (Windows, Linux, macOS)
- Different Python versions installed
- Package version conflicts
- Missing system dependencies
- Environment variables not set

**Impact**: 
- Code works for some developers but not others
- Difficult to reproduce bugs
- Onboarding new developers is slow
- Production deployments fail due to environment differences

**Solution**: 
- Use Docker Compose for consistent environments
- Pin Python version (3.11)
- Use requirements.txt with version pinning
- Document all dependencies
- Use .env.example for environment variables
- Docker ensures same environment for all

**Prevention**: 
- Always use Docker for development
- Version control all configuration files
- Document setup process
- Test on clean environments

**Code Example**:
```yaml
# docker-compose.yml
services:
  kafka:
    image: confluentinc/cp-kafka:7.5.0  # Pinned version
    environment:
      KAFKA_NODE_ID: 1
      # All config in one place
```

---

### Challenge 2: Infrastructure Setup Complexity

**Problem**: 
Setting up Kafka, MongoDB, PostgreSQL manually is time-consuming and error-prone. Each developer spends hours configuring services.

**Why It Happens**: 
- Multiple services need configuration
- Service dependencies (Kafka needs time to start)
- Configuration files scattered
- Manual setup steps
- Version compatibility issues

**Impact**: 
- Slow onboarding (days instead of hours)
- Configuration errors
- Inconsistent setups
- Time wasted on setup instead of development

**Solution**: 
- Docker Compose orchestrates all services
- Single command: `docker-compose up -d`
- Health checks ensure services are ready
- Automated initialization scripts
- Clear documentation

**Prevention**: 
- Infrastructure as code (Docker Compose)
- Automated setup scripts
- Health checks
- Clear README with setup steps

**Code Example**:
```bash
# Single command setup
docker-compose up -d

# Health checks ensure services ready
healthcheck:
  test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
  interval: 30s
  retries: 5
```

---

### Challenge 3: Project Structure Confusion

**Problem**: 
New developers don't know where to find code, where to add features, or how the project is organized.

**Why It Happens**: 
- No clear structure
- Files scattered
- No documentation
- Unclear separation of concerns
- Inconsistent naming

**Impact**: 
- Slow development
- Code in wrong places
- Difficult to maintain
- Hard to scale
- Team confusion

**Solution**: 
- Clear folder structure by concern
- Each module has specific purpose
- Documentation explains structure
- Consistent naming conventions
- Separation: data_generator, kafka, spark_streaming, etc.

**Prevention**: 
- Document structure in README
- Follow conventions
- Code reviews check structure
- Onboarding guide

**Code Example**:
```
iot-data-pipeline/
├── data_generator/    # Topic 2: Data generation
├── kafka/             # Topic 2: Kafka configs
├── spark_streaming/    # Topic 3: Real-time processing
├── spark_batch/        # Topic 4: Batch processing
├── data_quality/      # Topic 5: Validation
├── dbt/               # Topic 6: Transformations
├── api/               # Topic 7: REST API
├── airflow/           # Topic 8: Orchestration
├── monitoring/        # Topic 9: Logging & alerts
├── docker/            # Topic 10: Containerization
└── docs/              # Documentation
```

---

### Challenge 4: Service Dependencies and Startup Order

**Problem**: 
Services start in wrong order, causing failures. Kafka tries to connect before it's ready, consumers fail before topics exist.

**Why It Happens**: 
- Services have dependencies
- No startup order defined
- Race conditions
- Health checks not configured
- Services not ready when dependencies start

**Impact**: 
- Intermittent failures
- Manual intervention needed
- Unreliable startup
- Difficult to debug

**Solution**: 
- Use `depends_on` in Docker Compose
- Health checks ensure readiness
- Wait scripts for service readiness
- Retry logic in applications
- Proper error handling

**Prevention**: 
- Define dependencies clearly
- Use health checks
- Test startup scenarios
- Document dependencies

**Code Example**:
```yaml
kafka:
  healthcheck:
    test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
  depends_on:
    - mongodb
    - postgresql
```

---

### Challenge 5: Configuration Management

**Problem**: 
Configuration scattered across files, hardcoded values, secrets in code, different configs for dev/prod.

**Why It Happens**: 
- No centralized config
- Hardcoded values
- Secrets in code
- No environment separation
- Configuration drift

**Impact**: 
- Security issues (exposed secrets)
- Difficult to change configs
- Environment-specific bugs
- Deployment failures

**Solution**: 
- Environment variables (.env file)
- .env.example template
- No secrets in code
- Configuration via environment
- Separate dev/prod configs

**Prevention**: 
- Use .env for all configs
- Never commit .env
- Use .env.example as template
- Document all variables
- Secrets management (AWS Secrets Manager in prod)

**Code Example**:
```python
# Load from environment
import os
from dotenv import load_dotenv

load_dotenv()
kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
```

---

## ❓ WHY, WHAT, HOW TO, WHAT IF - COMPREHENSIVE EXPLANATIONS

### WHY This Folder Structure?

**Business Reason**: 
- Faster development (developers know where to find code)
- Easier onboarding (clear structure)
- Better collaboration (teams work on different modules)
- Reduced bugs (clear separation prevents mistakes)

**Technical Reason**: 
- Separation of concerns (each module has one responsibility)
- Scalability (modules can scale independently)
- Testability (easier to test isolated modules)
- Maintainability (changes isolated to one module)

**Trade-offs**: 
- Gain: Maintainability, scalability, clarity
- Lose: Slight overhead (more folders), need to understand structure

**Alternatives Considered**: 
- Monolithic structure (all code in one place)
- Microservices structure (each service separate repo)
- Layer-based structure (by technical layer)

**Why Not Alternatives**: 
- Monolithic: Hard to scale, maintain, test
- Microservices: Overkill for this project, adds complexity
- Layer-based: Doesn't match our use case (we have distinct topics)

---

### WHAT is Docker Compose?

**Definition**: 
Docker Compose is a tool for defining and running multi-container Docker applications using a YAML file.

**Purpose**: 
Orchestrates multiple services (Kafka, MongoDB, PostgreSQL) with a single command, managing dependencies, networks, and volumes.

**How It Works**: 
1. Define services in docker-compose.yml
2. Run `docker-compose up`
3. Docker Compose creates network for services
4. Starts services in dependency order
5. Manages volumes for data persistence
6. Handles service discovery (services can communicate by name)

**Key Components**: 
- Services (containers to run)
- Networks (communication between services)
- Volumes (data persistence)
- Environment variables (configuration)
- Health checks (service readiness)

**Types/Variants**: 
- Docker Compose (local development)
- Kubernetes (production orchestration)
- Docker Swarm (simpler orchestration)

**Real-World Analogy**: 
Like a restaurant manager coordinating multiple chefs (services) - ensures each chef has ingredients (dependencies), coordinates timing (startup order), and manages the kitchen (network).

---

### HOW TO Set Up the Project?

**Step 1**: Clone repository and navigate to project
```bash
git clone <repository-url>
cd "IOT Data Engineering Project"
```

**Step 2**: Create .env file from template
```bash
cp .env.example .env
# Edit .env with your configuration
```

**Step 3**: Start infrastructure services
```bash
cd docker
docker-compose up -d
```

**Step 4**: Wait for services to be ready
```bash
# Check service health
docker-compose ps

# Wait for Kafka (may take 30-60 seconds)
docker logs iot_kafka
```

**Step 5**: Initialize Kafka topics
```bash
cd ../kafka
pip install -r requirements.txt
python init_topics.py
```

**Step 6**: Verify setup
```bash
# Check all services running
docker ps

# Test Kafka
docker exec -it iot_kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Best Practices**: 
- Always use Docker for consistency
- Check service health before proceeding
- Use .env for configuration
- Document any manual steps
- Test on clean environment

**Common Mistakes**: 
- Starting services before dependencies ready
- Forgetting to create .env file
- Not waiting for Kafka to be ready
- Using wrong Python version
- Not checking service health

**Code Example**: Complete setup script in `scripts/setup.py`

---

### WHAT IF Docker Services Fail to Start?

**Problem**: 
Docker services don't start, containers crash, or services are unhealthy.

**Detection**: 
- `docker-compose ps` shows unhealthy services
- `docker logs <service>` shows errors
- Health checks fail
- Ports not accessible

**Impact**: 
- Cannot run application
- Development blocked
- Need to debug infrastructure

**Solution**: 
1. Check Docker daemon is running: `docker info`
2. Check logs: `docker logs <service>`
3. Check port conflicts: `netstat -an | grep <port>`
4. Check disk space: `df -h`
5. Check memory: `docker stats`
6. Restart services: `docker-compose restart`
7. Rebuild if needed: `docker-compose up -d --build`

**Prevention**: 
- Monitor Docker daemon
- Check system resources
- Avoid port conflicts
- Keep Docker updated
- Use health checks

**Monitoring**: 
- Docker health checks
- Service logs
- Resource usage
- Port availability

**Interview Answer**: 
"If Docker services fail, I first check Docker daemon is running and has resources. Then I check service logs to identify the error. Common issues are port conflicts, insufficient memory, or configuration errors. I use health checks to detect issues early and set up monitoring for production."

---

### WHAT IF Multiple Developers Work on Same Project?

**Problem**: 
Code conflicts, different environments, inconsistent configurations, merge conflicts.

**Detection**: 
- Git merge conflicts
- Different behavior on different machines
- Configuration differences
- Dependency version conflicts

**Impact**: 
- Development delays
- Bugs that only appear on some machines
- Difficult collaboration
- Integration issues

**Solution**: 
1. Use Git for version control
2. Docker ensures consistent environments
3. .env.example for configuration template
4. requirements.txt with pinned versions
5. Clear branching strategy
6. Code reviews
7. CI/CD for automated testing

**Prevention**: 
- Docker for environment consistency
- Version control all code
- Document setup process
- Use .env.example
- Pin dependency versions
- Clear Git workflow

**Monitoring**: 
- Git commit history
- CI/CD pipeline results
- Code review process

**Interview Answer**: 
"We use Docker to ensure all developers have identical environments. Git manages code versioning with a clear branching strategy. Configuration is externalized via .env files. Dependencies are pinned in requirements.txt. CI/CD pipeline tests code automatically. This ensures consistency and prevents 'works on my machine' issues."

---

## 🎤 INTERVIEW PREPARATION - COMPREHENSIVE Q&A

### Basic Questions

**Q: "Why did you choose this project structure?"**

**A**: 
"We use a modular structure where each folder represents a distinct concern or topic. This separation allows teams to work independently, makes the codebase maintainable, and enables each component to scale separately. For example, data_generator is separate from spark_streaming because they have different dependencies and can be deployed independently."

**Key Points**:
- Separation of concerns
- Independent scaling
- Team collaboration
- Maintainability
- Clear responsibilities

**Follow-up Questions**:
- "How would you scale this structure?"
- "What if you had 100 developers?"
- "How do you handle dependencies between modules?"

**Deep Dive**: 
The structure follows microservices principles but in a monorepo. Each module can be:
- Developed independently
- Tested in isolation
- Deployed separately (if needed)
- Scaled independently
- Maintained by different teams

**Code Example**: 
Each module has its own requirements.txt, can run independently, and has clear interfaces.

**Real-World Example**: 
Similar to how Netflix structures their data platform - separate services for ingestion, processing, storage, serving, orchestration.

---

### System Design

**Q: "Design a data engineering project structure for a team of 10 developers"**

**A**: 
"Step 1: Separate by domain (ingestion, processing, storage, serving). Step 2: Each domain has its own folder with clear interfaces. Step 3: Shared utilities in common folder. Step 4: Documentation in docs/. Step 5: Tests alongside code. Step 6: CI/CD configuration in .github/. Step 7: Infrastructure as code in docker/."

**Components**:
- Domain folders (data_generator, spark_streaming, etc.)
- Shared utilities (monitoring, common functions)
- Documentation (docs/)
- Infrastructure (docker/)
- CI/CD (.github/)

**Trade-offs**:
- More folders = better organization but more navigation
- Monorepo = easier dependency management but larger repo
- Separate repos = better isolation but harder dependencies

**Scalability**:
- Add new domains as new folders
- Scale teams by domain ownership
- Independent deployment per domain

**Reliability**:
- Clear interfaces between domains
- Version control
- Testing at domain boundaries
- Documentation for each domain

---

### Troubleshooting

**Q: "How would you debug a setup issue where services don't start?"**

**A**: 
"Step 1: Check Docker daemon is running (`docker info`). Step 2: Check service logs (`docker logs <service>`). Step 3: Check resource availability (memory, disk). Step 4: Check port conflicts. Step 5: Check configuration files. Step 6: Check dependencies (are dependent services ready?). Step 7: Check health checks."

**Tools**:
- `docker-compose ps` - Service status
- `docker logs <service>` - Service logs
- `docker stats` - Resource usage
- `netstat` - Port conflicts
- `docker-compose config` - Validate config

**Steps**:
1. Verify Docker daemon
2. Check service logs for errors
3. Verify resources (CPU, memory, disk)
4. Check port availability
5. Validate configuration
6. Check service dependencies
7. Review health check results

**Common Causes**:
- Docker daemon not running
- Insufficient resources
- Port conflicts
- Configuration errors
- Dependency services not ready
- Network issues

**Prevention**:
- Health checks
- Resource monitoring
- Configuration validation
- Clear error messages
- Documentation

---

### Scenario-Based

**Q: "What if a new developer joins and can't set up the project?"**

**A**: 
"This indicates setup documentation or process issues. I'd: 1) Review setup documentation for clarity, 2) Test setup on clean environment, 3) Identify pain points, 4) Improve documentation, 5) Add setup script automation, 6) Create troubleshooting guide, 7) Pair with new developer to fix issues."

**Prevention**:
- Clear README with step-by-step instructions
- Automated setup scripts
- Docker for environment consistency
- .env.example template
- Troubleshooting guide
- Onboarding checklist

**Monitoring**:
- Track onboarding time
- Collect feedback
- Monitor setup failures
- Update documentation based on issues

**Trade-offs**:
- More documentation = better onboarding but maintenance overhead
- Automation = faster setup but more complex scripts
- Docker = consistency but requires Docker knowledge

---

## 🧪 LEARNING-BY-DOING EXERCISES

### Exercise 1: Set Up Project from Scratch

**Objective**: 
Successfully set up the entire project infrastructure on a clean machine.

**Steps**:
1. Clone repository
2. Install Docker and Docker Compose
3. Copy .env.example to .env
4. Run `docker-compose up -d`
5. Wait for services to be healthy
6. Initialize Kafka topics
7. Verify all services running

**Expected Result**: 
All services running, Kafka topics created, can access services on expected ports.

**Common Issues**:
- Docker not installed
- Port conflicts (27017, 5432, 9092)
- Insufficient memory
- Services not ready when dependencies start

**Solution**:
- Install Docker Desktop
- Change ports in docker-compose.yml if conflicts
- Increase Docker memory allocation
- Use health checks and wait scripts

**Verification**:
```bash
# Check all services
docker-compose ps

# Test MongoDB
docker exec -it iot_mongodb mongosh --eval "db.adminCommand('ping')"

# Test PostgreSQL
docker exec -it iot_postgresql psql -U postgres -c "SELECT version();"

# Test Kafka
docker exec -it iot_kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

### Exercise 2: Simulate Service Failure

**Objective**: 
Understand how the system handles service failures and recovery.

**Steps**:
1. Start all services
2. Stop Kafka: `docker stop iot_kafka`
3. Try to send data (observe producer behavior)
4. Restart Kafka: `docker start iot_kafka`
5. Verify data flow resumes

**Expected Result**: 
Producer handles failure gracefully (retries, queues), resumes when Kafka recovers, no data loss.

**Common Issues**:
- Producer crashes instead of handling failure
- Messages lost during outage
- Producer doesn't resume after recovery

**Solution**:
- Implement proper error handling
- Use local queue for resilience
- Implement health checks
- Test failure scenarios

---

### Common Mistake: Hardcoding Configuration

**What Happens**: 
Configuration values hardcoded in code (e.g., `bootstrap_servers = "localhost:9092"`).

**Why**: 
Quick development, didn't think about different environments.

**Impact**: 
- Can't change config without code change
- Different configs for dev/prod require code changes
- Security issues if secrets hardcoded
- Difficult to test with different configs

**Fix**: 
- Use environment variables
- Load from .env file
- Provide defaults
- Document all configs

**Prevention**: 
- Never hardcode configs
- Always use environment variables
- Use .env.example as template
- Code review checks for hardcoded values

---

### Common Mistake: Not Waiting for Services

**What Happens**: 
Scripts try to connect to services before they're ready, causing connection errors.

**Why**: 
Services take time to start, scripts don't wait.

**Impact**: 
- Intermittent failures
- Need to retry manually
- Unreliable automation
- Difficult to debug

**Fix**: 
- Use health checks
- Wait scripts
- Retry logic in applications
- Check service status before connecting

**Prevention**: 
- Always use health checks
- Implement wait logic
- Test startup scenarios
- Document startup time

---

## 🚀 MODERN DATA ENGINEERING PRACTICES

### Industry Standards

**Current Practices (2024-2025)**:
- Docker for containerization (industry standard)
- Docker Compose for local development
- Kubernetes for production orchestration
- Infrastructure as Code (IaC)
- Git for version control
- CI/CD for automation
- Environment-based configuration
- Health checks for reliability

**Project Alignment**:
- ✅ Docker Compose for local dev
- ✅ Git for version control
- ✅ Environment variables for config
- ✅ Health checks implemented
- ⏳ Kubernetes for production (future)
- ⏳ CI/CD pipeline (Topic 11)

---

### Cloud Deployment

**AWS Example**:
- **ECS/EKS**: Container orchestration (instead of Docker Compose)
- **RDS**: Managed PostgreSQL
- **DocumentDB**: Managed MongoDB
- **MSK**: Managed Kafka
- **ECR**: Container registry
- **Secrets Manager**: For secrets
- **CloudWatch**: Monitoring

**GCP Example**:
- **Cloud Run**: Serverless containers
- **Cloud SQL**: Managed PostgreSQL
- **MongoDB Atlas**: Managed MongoDB
- **Pub/Sub**: Message queue (alternative to Kafka)
- **Artifact Registry**: Container registry
- **Secret Manager**: For secrets
- **Cloud Monitoring**: Observability

**Azure Example**:
- **Azure Container Instances**: Container hosting
- **Azure Database for PostgreSQL**: Managed PostgreSQL
- **Cosmos DB**: Managed NoSQL
- **Event Hubs**: Message queue
- **Container Registry**: Container registry
- **Key Vault**: Secrets management
- **Azure Monitor**: Observability

**Migration Path**:
1. Containerize all services (Docker)
2. Push images to cloud registry
3. Use managed services (RDS, MSK)
4. Deploy containers to ECS/EKS
5. Use cloud secrets management
6. Set up cloud monitoring

---

### Observability

**Modern Tools**:
- **Grafana**: Dashboards and visualization
- **Prometheus**: Metrics collection
- **ELK Stack**: Log aggregation (Elasticsearch, Logstash, Kibana)
- **Jaeger**: Distributed tracing
- **Datadog**: All-in-one observability
- **New Relic**: APM and monitoring

**What to Monitor**:
- Service health (up/down)
- Resource usage (CPU, memory, disk)
- Request rates and latency
- Error rates
- Data quality metrics
- Pipeline health

**Implementation**:
- Structured logging (JSON)
- Metrics collection (Prometheus)
- Distributed tracing (Jaeger)
- Dashboards (Grafana)
- Alerts (PagerDuty, Slack)

---

### Cost Optimization

**Strategies**:
- Use spot instances for non-critical workloads
- Right-size resources (not over-provisioning)
- Auto-scaling based on load
- Data retention policies (delete old data)
- Compression (reduce storage)
- Use managed services (reduce operational overhead)

**Cost Breakdown** (Example for AWS):
- Compute (ECS/EKS): $200-500/month
- Databases (RDS + DocumentDB): $300-800/month
- Kafka (MSK): $400-1000/month
- Storage: $50-200/month
- Network: $20-100/month
- **Total**: ~$1000-2600/month for small scale

**Optimization**:
- Use reserved instances (30-40% savings)
- Auto-scale down during low usage
- Archive old data to S3 (cheaper)
- Use spot instances for batch jobs
- Monitor and optimize continuously

---

### Performance

**Optimization Techniques**:
- Connection pooling (database connections)
- Caching (Redis for frequently accessed data)
- Batch processing (reduce I/O)
- Compression (reduce network/storage)
- Indexing (faster queries)
- Partitioning (parallel processing)

**Bottleneck Identification**:
- Profile code (identify slow parts)
- Monitor resource usage
- Track latency metrics
- Analyze query performance
- Review network I/O

---

### Security

**Best Practices**:
- Never commit secrets to Git
- Use secrets management (AWS Secrets Manager)
- Encrypt data at rest and in transit
- Network isolation (VPC)
- Access controls (IAM, RBAC)
- Regular security audits
- Dependency scanning

**Implementation**:
- .env files for local (not committed)
- Secrets Manager for production
- TLS for all connections
- VPC for network isolation
- IAM roles for access control
- Regular dependency updates

---

### Compliance

**GDPR Considerations**:
- Data retention policies
- Right to deletion
- Data encryption
- Access logging
- Consent management

**HIPAA Considerations** (if healthcare data):
- Encryption requirements
- Access controls
- Audit logging
- Data residency

**Implementation**:
- Data retention: 7 days for Kafka, configurable for databases
- Encryption: TLS for all connections
- Access logs: All access logged
- Deletion: Can delete user data on request

---

### Data Governance

**Components**:
- **Data Lineage**: Track data flow (where data comes from, where it goes)
- **Data Catalog**: Document all datasets
- **Data Quality**: Monitor and track quality metrics
- **Access Control**: Who can access what data
- **Audit Trail**: Track all data access and changes

**Tools**:
- **Apache Atlas**: Data governance and lineage
- **Collibra**: Data catalog
- **Great Expectations**: Data quality
- **dbt**: Data lineage (built-in)

**Implementation**:
- Document data flow in architecture.md
- Track data quality metrics
- Log all data access
- Version control schemas
- Document data dictionaries

---

## Summary

Topic 1 establishes the foundation for the entire project:

✅ **Consistent Environments**: Docker ensures all developers have same setup
✅ **Clear Structure**: Modular organization enables scalability
✅ **Infrastructure as Code**: Docker Compose manages all services
✅ **Configuration Management**: Environment variables for flexibility
✅ **Documentation**: Clear setup and architecture documentation

**Key Takeaways**:
- Good foundation enables faster development
- Clear structure prevents confusion
- Docker solves environment consistency
- Documentation is critical for onboarding
- Infrastructure as code is essential

---

**Last Updated**: January 2025
**Topic**: 1 - Project Setup & Architecture

