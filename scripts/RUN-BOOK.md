# Run-Book: NYC Transportation Project — Deployment & Debugging Log

> **Purpose of this document:** A complete record of every issue encountered while trying to get the Chapter 13 NYC Transportation project (taxi vs. citibike comparison) running, the solutions applied, and the hidden specializations required. If you are starting from the original project snapshot, read this first.

---

## Table of Contents

1. [The Original Project (Starting Point)](#1-the-original-project-starting-point)
2. [Phase 1: Local Build Attempt — "Potato PC"](#2-phase-1-local-build-attempt--potato-pc)
3. [Phase 2: Move Builds to GitHub Actions](#3-phase-2-move-builds-to-github-actions)
4. [Phase 3: Codespaces — Resource Wall](#4-phase-3-codespaces--resource-wall)
5. [Phase 4: Docker Networking — The Two-Firewall Trap](#5-phase-4-docker-networking--the-two-firewall-trap)
6. [Hidden Specializations Required (Beyond Project Tools)](#6-hidden-specializations-required-beyond-project-tools)
7. [Codespaces Free Tier: What You Actually Get](#7-codespaces-free-tier-what-you-actually-get)
8. [Laptop-Less Development Setup: Reality Check](#8-laptop-less-development-setup-reality-check)
9. [Final Working Configuration (All Changes Applied)](#9-final-working-configuration-all-changes-applied)
10. [Lessons Learned & Time Audit](#10-lessons-learned--time-audit)

---

## 1. The Original Project (Starting Point)

The project is a docker-compose-based data pipeline from the *Data Pipelines with Apache Airflow* book (Chapter 13). It includes:

| Service | Purpose | Port |
|---|---|---|
| Airflow (CeleryExecutor) | Orchestrates DAGs | 8080 |
| PostgreSQL (×4) | Airflow metadata, taxi DB, citibike DB, result DB | 5432–5435 |
| Redis | Celery broker | 6379 |
| MinIO | S3-compatible object store | 9000, 9001 |
| Taxi Fileserver (nginx) | Serves CSV files of taxi rides | 8081 |
| Citibike API (Flask) | Serves JSON of citibike rides | 8082 |
| NYC Transportation API (Flask) | Final dashboard | 8083 |

**Original `docker compose up --build`** attempted to:

- Download **~4 GB** of NYC taxi parquet files from CloudFront (CDN) during image build
- Download **~1 GB** of citibike trip data from S3
- Build custom Docker images for 6 services
- Run Airflow with CeleryExecutor, worker, scheduler, triggerer, dag-processor, apiserver — **6 Airflow containers alone**

The README says `docker compose up -d --build` and "the final result is visible on http://localhost:8083." It does not mention resource requirements, networking complexity, or build time.

---

## 2. Phase 1: Local Build Attempt — "Potato PC"

### Issue

Running `docker compose up --build` on a local PC with limited hardware and West African internet connection led to:

- **Pip install timeouts** — Airflow 3.x and dependencies (geopandas, pyarrow, pandas, etc.) are large; slow connections caused repeated retries and failures.
- **Docker pull timeouts** — Base images (Postgres, Redis, Airflow) are hundreds of MB each; pulling them over a slow link took hours or failed.
- **Cross-continental CDN latency** — The taxi data (`d37ci6vzurychx.cloudfront.net`) and citibike data (`s3.amazonaws.com`) were slow to reach West Africa, and the build process downloads them **every time** (no caching at the Docker layer for external URLs).

### Solution Applied

Move the build off the local machine entirely. Build Docker images **once on GitHub Actions** (which has fast, cached infrastructure), push them to **GitHub Container Registry (GHCR)**, and have Codespaces (or any machine) simply **pull** the pre-built images.

**What changed:**
- Added `.github/workflows/build-images.yaml` — a matrix build that builds and pushes all 6 custom images to `ghcr.io/samuel-jinadu/nyc-*:latest`.
- `compose.override.yaml` changed from `build: context: ./services/...` to `image: ghcr.io/samuel-jinadu/nyc-...:latest`.
- `minio_init` changed from `minio/mc:latest` to `pgsty/mc:latest` (because MinIO **removed** the official `minio/mc` image in 2025–2026; `pgsty/mc` is the actively maintained fork).

**Result:** Local build no longer needed. Codespaces can pull images in minutes instead of building for hours.

---

## 3. Phase 2: Move Builds to GitHub Actions

### What the Workflow Does

```yaml
name: Build and Push Images to GHCR
on:
  workflow_dispatch:
  push:
    branches: [main]
    paths: ['airflow/**', 'services/**', '.github/workflows/build-images.yml']
jobs:
  build:
    strategy:
      matrix:
        include:
          - image: airflow
            context: .
            file: ./airflow/Dockerfile
          - image: taxi_db
            context: ./services/taxi_db
            file: ./services/taxi_db/Dockerfile
          # ... taxi_fileserver, citibike_api, citibike_db, nyc_transportation_api
```

Each image is built with `docker/build-push-action@v5` and pushed to `ghcr.io/${{ github.repository_owner }}/nyc-${{ matrix.image }}:latest`.

**Trade-off:** GitHub Actions free tier has **2,000 minutes/month** for free accounts (public repos get unlimited). Building 6 images with heavy dependencies (Airflow, geopandas, pyarrow) takes ~10–15 minutes per build. This is fine for occasional rebuilds but not for rapid iteration.

**Hidden requirement:** You need to understand **GitHub Actions matrix builds**, **GHCR authentication**, and **Docker BuildKit caching** (`cache-from: type=gha`, `cache-to: type=gha,mode=max`) to make this efficient.

---

## 4. Phase 3: Codespaces — Resource Wall

### Issue

Codespaces default machines are **2 cores / 8 GB RAM / ~32 GB disk**. Running the full stack:

- **6 Airflow containers** (apiserver, scheduler, worker, triggerer, dag-processor, init)
- **4 PostgreSQL databases**
- **Redis**
- **MinIO**
- **Taxi fileserver, Citibike API, NYC Transportation API**
- **Taxi DB and Citibike DB** — each is a **custom-built image with embedded data** (taxi_db is ~1 GB, citibike_db is ~890 MB)

**Total: 16 containers**, several of which are memory-hungry. The Airflow scheduler alone recommends 2–4 GB. The 8 GB machine ran out of memory and disk (logs showed `WARNING: Not enough Disk space available for Docker. At least 10 GBs recommended. You have 6.0G`).

### Solution Applied

Upgrade to a **4-core / 16 GB RAM** Codespaces machine. This consumed more core-hours (4 core-hours per real hour vs. 2 for the 2-core machine), but was necessary to run the stack.

**Cost implication:** On the free tier (120 core-hours/month), a 4-core machine gives you **30 real hours** of runtime per month. That is roughly **1 hour per day** if you use it every day. This is tight but workable if you stop the Codespace when not actively working.

### Additional Resource Considerations

- **Disk:** Codespaces gives **15 GB of storage** on the free tier. The Docker images alone (taxi_db ~1 GB, citibike_db ~890 MB, Airflow ~2 GB, Postgres, Redis, MinIO) plus volumes (postgres data, MinIO data) consume most of this. The `docker system prune -af` command was run multiple times to free space.
- **Memory:** The Airflow init container printed warnings about insufficient resources. Upgrading to 16 GB resolved this.
- **Startup time:** Even after upgrading, the full stack takes **2–4 minutes** to become healthy (Postgres initialization, Airflow DB migration, MinIO healthcheck).

---

## 5. Phase 4: Docker Networking — The Two-Firewall Trap

This was the **hardest and most time-consuming issue** — the one that took two days to resolve.

### Symptom

Containers could not communicate with each other:

```
minio_init-1 | mc: <ERROR> Unable to initialize new alias from the provided credentials. Get "http://minio:9000/...": dial tcp 10.99.0.4:9000: i/o timeout.
```

- DNS resolved correctly (`minio` → `10.99.0.4`, `postgres` → `10.99.0.x`)
- Containers could reach themselves (`curl http://10.99.0.4:9000` from inside MinIO returned `200 OK`)
- Published ports worked (MinIO's own healthcheck passed, `8082`/`8083` responded from the host)
- **But no container could reach any other container on any port**

### Diagnostic Steps That Failed (and Why)

| Attempt | What We Tried | Why It Didn't Work |
|---|---|---|
| 1 | Check Docker DNS (`getent hosts minio`) | DNS was fine — resolved to correct IP |
| 2 | Test TCP with `/dev/tcp` | `minio_init`'s shell supports it; showed `CLOSED` for all peers |
| 3 | Change subnet from `172.18.0.0/16` to `10.99.0.0/24` | Subnet was never the problem — drop was address-independent |
| 4 | `sudo iptables -P FORWARD ACCEPT` | Changed **nftables** policy, not the active stack |
| 5 | `docker network inspect` | Both containers on same network, correct IPs |
| 6 | Fresh Alpine containers on a new network | Also failed — confirmed daemon-level issue |

### The Actual Root Cause

**Two parallel firewall stacks were loaded in the kernel:**

```
# Warning: iptables-legacy tables present, use iptables-legacy to see them
```

This warning appeared in **every** `iptables` command output, but was initially overlooked.

- **nftables** — Docker's rules. `FORWARD` policy was `ACCEPT`, `DOCKER-USER` was empty.
- **iptables-legacy** — Codespaces' pre-installed rules. `FORWARD` policy was **`DROP`** (2697 packets had already been dropped by it).

**Why this breaks everything:**

- Docker on Ubuntu 22+ uses **nftables** by default.
- Codespaces' base VM ships with **legacy iptables rules** (from its own networking setup).
- Both stacks are live. The kernel evaluates **both**. Whichever has the stricter policy wins.
- The legacy `FORWARD` chain had **no rules for Docker's custom bridge** (`br-823b08009ac5`), so all inter-container packets fell through to the `DROP` policy.
- `iptables -P FORWARD ACCEPT` only changed the **nft** side. The legacy side stayed `DROP`.

### The Fix

```bash
sudo iptables-legacy -P FORWARD ACCEPT
sudo iptables-legacy -I FORWARD 1 -i br-+ -j ACCEPT
sudo iptables-legacy -I FORWARD 1 -o br-+ -j ACCEPT
```

After this, `minio_init` immediately succeeded:

```
minio_init-1 | Added `minio` successfully.
minio_init-1 | Bucket created successfully `minio/datalake`.
minio_init-1 | Exited (0)
```

### Why This Is a Codespaces-Specific Problem

This is **not** a normal Docker issue. On a local Linux machine, Docker installs its own iptables rules during daemon startup, and the `FORWARD` chain is configured correctly. On Codespaces:

- The base VM has **pre-existing legacy iptables rules** for its own networking.
- Docker starts and installs **nftables** rules.
- The legacy rules are **never cleaned up**, and the legacy `FORWARD` policy remains `DROP`.
- Docker's `DOCKER-USER` chain (which normally allows you to add rules) is **empty** in the legacy stack, so there's no place for Docker to insert its accept rules.

This is a known class of issue. The Tailscale project explicitly documented that "GitHub codespaces ... have pre-existing legacy iptables rules in the IPv4 tables, as such the nascent firewall mode detection will always pick iptables." And there's a GitHub community discussion from **September 2026** with the exact same symptom: "Docker container-to-container connection issue in GitHub Codespaces (works fine locally)."

### Persistent Fix

Add to `.devcontainer/devcontainer.json`:

```json
{
  "postStartCommand": "sudo iptables-legacy -P FORWARD ACCEPT && sudo iptables-legacy -I FORWARD 1 -i br-+ -j ACCEPT && sudo iptables-legacy -I FORWARD 1 -o br-+ -j ACCEPT || true"
}
```

This runs **every time the Codespace starts**, before you run `docker compose up`.

---

## 6. Hidden Specializations Required (Beyond Project Tools)

To solve this chain of issues, you needed knowledge from **outside** the project's stated tech stack (Airflow, Python, SQL, Docker Compose):

| Issue | Required Specialization | Project Didn't Mention |
|---|---|---|
| Slow pip/docker pulls | **CI/CD pipelines** (GitHub Actions matrix builds, GHCR authentication, BuildKit caching) | No mention of where to build images |
| Cross-continental CDN | **Cloud networking / edge caching** (CloudFront, S3, bandwidth optimization) | Assumed fast internet |
| Potato PC | **Cloud development environments** (Codespaces machine types, resource limits, core-hour accounting) | Assumed capable local machine |
| Docker networking | **Linux kernel networking** (nftables vs. iptables-legacy, bridge networking, FORWARD chain, `DOCKER-USER`) | Assumed `docker compose up` just works |
| Two-firewall trap | **Debugging methodology** (systematic elimination, reading warnings, checking both firewall backends) | No troubleshooting guide |
| Disk full | **Docker storage management** (`docker system prune`, volume cleanup, image size optimization) | No resource guidance |
| MinIO client removed | **Tracking upstream project changes** (MinIO archived the official `mc` image; `pgsty/mc` is the fork) | Assumed `minio/mc` still exists |

**In short:** You needed to be a **DevOps engineer**, **network engineer**, **CI/CD specialist**, and **Linux sysadmin** — not just a data engineer.

---

## 7. Codespaces Free Tier: What You Actually Get

### The Numbers

| Plan | Core Hours / Month | Storage / Month | Real Runtime (2-core) | Real Runtime (4-core) |
|---|---|---|---|---|
| **GitHub Free** | 120 core-hours | 15 GB | ~60 hours | ~30 hours |
| **GitHub Pro** | 180 core-hours | 20 GB | ~90 hours | ~45 hours |
| **Student (via Education Pack)** | 180 core-hours | 20 GB | ~90 hours | ~45 hours |

**Source:** GitHub community discussions confirm the free tier for personal accounts is **120 core-hours and 15 GB storage**. On a 2-core machine, this equals **60 real hours**; on a 4-core machine, **30 real hours**.

### Important Caveats

- **"Active" means running, not editing.** If the Codespace is running (even with no keyboard input), you consume core-hours. The default idle timeout is **30 minutes**, but running processes (like Airflow) keep it active.
- **Stopped Codespaces still consume storage.** Compute stops when the Codespace is stopped, but storage persists until the Codespace is **deleted**. A stopped Codespace with a 15 GB Docker volume still counts against your 15 GB quota.
- **4-core machines burn core-hours 2× faster.** You upgraded to 4-core/16 GB to run the stack, which means your free tier gives you **~30 real hours/month** — about **1 hour per day** if used daily.
- **If you exceed the quota, Codespaces stops.** It does **not** automatically charge you (unless you've set a spending limit above $0). With a $0 spending limit, the Codespace simply stops and you must wait until the next month or upgrade.

### Is This Sustainable for Development?

**For this specific project, no.** The full stack is too heavy:

- 16 containers
- 4 PostgreSQL databases
- MinIO with persistent volume
- 6 Airflow services
- ~6 GB of Docker images

Even on 4-core/16 GB, you'll spend most of your 30 hours just keeping the stack alive. **You cannot afford to leave it running overnight or while idle.**

**Workarounds:**

1. **Stop the Codespace manually** when you step away — don't rely on the 30-minute idle timeout.
2. **Delete old Codespaces** to free storage quota — stopped Codespaces still consume storage.
3. **Use `docker compose down -v`** to remove volumes when you're done for the day.
4. **Request the GitHub Student Developer Pack** — if you're a student, you get **180 core-hours** (90 real hours on 2-core, 45 on 4-core) and 20 GB storage.
5. **Consider a lighter workflow:** Run only the services you need for a given task (e.g., just Airflow + MinIO + one Postgres) instead of the full stack.

---

## 8. Laptop-Less Development Setup: Reality Check

### What Works

Codespaces **is** a viable laptop-less development setup for many projects:

- **Low-spec hardware friendly** — you can develop from a Chromebook, tablet, or older laptop because the heavy lifting happens in the cloud.
- **Browser-based VS Code** — no installation needed.
- **Persistent environment** — your files, dependencies, and Docker images survive Codespace restarts (within the storage quota).
- **Port forwarding** — Codespaces automatically forwards ports (8080, 8081, etc.) to a public URL, so you can view the Airflow UI, APIs, and MinIO console from any browser.

### What Doesn't Work (For This Project)

- **Running the full stack for extended periods** — 30 real hours/month on the free tier is not enough for a 16-container stack that you need to keep alive while developing DAGs.
- **Heavy data processing** — geopandas spatial joins, parquet transformations, and 4 GB data downloads will exhaust the 4-core CPU and 16 GB RAM if you run them repeatedly.
- **Build-heavy workflows** — even with GitHub Actions pre-building images, the initial pull of 6+ GB of images consumes disk and time.

### Alternative Approaches

| Approach | Pros | Cons |
|---|---|---|
| **Codespaces (4-core)** | No local hardware needed; GitHub-integrated | 30 real hours/month on free tier; storage constraints |
| **GitHub Student Pack + Codespaces** | 180 core-hours (45 hours on 4-core) | Requires student verification |
| **Local machine + pre-built images** | No cloud quota limits | Requires a machine with 16 GB RAM and 20+ GB disk |
| **Cloud VM (e.g., AWS EC2 t3.xlarge)** | Full control; can run 24/7 | Costs money; you said you have zero budget |
| **Split workflow** | Develop DAGs in Codespaces with a **minimal** stack (Airflow + MinIO + 1 Postgres); run the full stack only when testing end-to-end | Requires discipline; not all tests will work |

**Recommendation for your situation:** Use Codespaces for **code editing and DAG development** with a **minimal subset** of services (Airflow + MinIO + result_db). Run the full stack only when you need to test the complete pipeline. Stop the Codespace immediately when you're done. Apply for the GitHub Student Pack if eligible. Delete old Codespaces to free storage.

---

## 9. Final Working Configuration (All Changes Applied)

### `compose.override.yaml` — Key Changes from Original

```yaml
x-airflow-common:
  &airflow-common
  environment:
    AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL: 60
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__CORE__TEST_CONNECTION: "Enabled"
    AIRFLOW__API_AUTH__JWT_SECRET: foo
    AIRFLOW__API__SECRET_KEY: bar
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "false"
    AIRFLOW__CORE__FERNET_KEY: hCRoPUYBO27QiEg1MRu5hSjLG7yNd8y8XKlm-8kRlkQ=
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "true"
    AIRFLOW_CONN_S3: aws://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY@/?endpoint_url=http%3A%2F%2Fminio%3A9000
    AIRFLOW_CONN_CITIBIKE: http://citibike:cycling@citibike_api:5000
    AIRFLOW_CONN_TAXI: http://taxi_fileserver
    AIRFLOW_CONN_RESULT_DB: postgresql://nyc:tr4N5p0RT4TI0N@result_db:5432/nyctransportation
  volumes:
    - ./src:/opt/airflow/nyctransport/src
    - ./setup.py:/opt/airflow/nyctransport/setup.py

services:
  # ... (all services use image: ghcr.io/... instead of build: context: ...)

  minio:
    image: minio/minio:RELEASE.2024-06-22T05-26-45Z
    # ... same as original

  minio_init:
    image: pgsty/mc:latest
    depends_on:
      - minio
    entrypoint: >
      /bin/sh -c "
      until (/usr/bin/mc alias set minio http://minio:9000 AKIAIOSFODNN7EXAMPLE wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY --insecure); do echo 'MinIO not up and running yet...' && sleep 1; done;
      /usr/bin/mc mb --ignore-existing minio/datalake --insecure;
      exit 0;
      "

volumes:
  locals3-data-volume:
  result-db-volume:
```

### `.devcontainer/devcontainer.json` — Persistent Firewall Fix

```json
{
  "postStartCommand": "sudo iptables-legacy -P FORWARD ACCEPT && sudo iptables-legacy -I FORWARD 1 -i br-+ -j ACCEPT && sudo iptables-legacy -I FORWARD 1 -o br-+ -j ACCEPT || true"
}
```

### Verification Commands

After starting the stack, verify inter-container networking:

```bash
docker compose exec airflow-worker curl -s http://minio:9000/minio/health/live
docker compose exec airflow-worker curl -s -o /dev/null -w '%{http_code}\n' http://citibike_api:5000/
docker compose exec airflow-worker curl -s http://taxi_fileserver/
docker compose exec airflow-worker bash -c 'echo > /dev/tcp/result_db/5432 && echo RESULT_DB_OPEN'
```

Expected: MinIO `200`, Citibike `401`, Taxi fileserver returns JSON file listing, Result DB `RESULT_DB_OPEN`.

---

## 10. Lessons Learned & Time Audit

### Time Spent (Approximate)

| Phase | Issue | Time |
|---|---|---|
| 1 | Local build attempt (slow downloads, pip failures) | ~4 hours |
| 2 | Setting up GitHub Actions build pipeline | ~2 hours |
| 3 | Codespaces resource wall (upgrade machine, free disk) | ~1 hour |
| 4 | **Docker networking debugging** | **~2 days** |
| 5 | Documentation (this run-book) | ~1 hour |

**Total: ~3 days before writing a single line of DAG code.**

### Why It Felt "Impossible"

The project README says "docker compose up -d --build" and "the final result is visible on http://localhost:8083." This creates the impression that it's a **5-minute setup**. In reality:

- It assumes a **fast internet connection** (multi-GB downloads).
- It assumes a **capable local machine** (16 GB RAM, 20+ GB disk).
- It assumes **Docker networking "just works"** (it doesn't on Codespaces due to the two-firewall trap).
- It assumes **MinIO's official `mc` image still exists** (it was removed in 2025–2026).
- It assumes **GitHub's CDN is fast worldwide** (it isn't, especially from West Africa).

None of these assumptions are stated. The project is a **teaching example** from a book, not a production-ready deployment. The book's authors likely ran it on a MacBook Pro with fiber internet.

### What Would Have Made This Easier

1. **A `requirements.txt` for infrastructure** — listing minimum RAM, disk, and internet speed.
2. **A troubleshooting section** in the README — covering Docker networking on cloud dev environments.
3. **Pre-built images on a public registry** — so users don't have to build.
4. **A "minimal stack" compose file** — Airflow + MinIO + one Postgres for DAG development.
5. **Explicit mention of the `iptables-legacy` issue** — this is a known Codespaces problem with a one-line fix.

### Key Takeaway

> **Running a multi-container data pipeline is not a "clone and run" experience on constrained infrastructure.** The gap between "the code works" and "the system runs" is filled with networking, resource management, and cloud-environment quirks that are never mentioned in tutorials. Budget time for infrastructure debugging — often more than for writing the actual application code.

---

*Run-book compiled from project snapshot, `docker ps` output, Codespaces terminal logs, and GitHub community documentation. Last updated: 2026-09-29.*