# Workload Identity Federation setup — target state, not yet applied

> **Status: DEFERRED — this is not a prerequisite for anything.**
>
> CI currently authenticates with the pre-existing `GCP_BIGQUERY_USER_KEYFILE`
> service account key (decision 9). Every step below requires IAM rights the
> maintainers do not hold — creating a service account, a workload identity
> pool, and a provider. Blocking the restoration of all test signal on an IT
> request was the worse trade.
>
> **Do this when someone with IAM admin on the CI project is already involved** —
> most naturally alongside the key rotation in [README.md](README.md) §1.1, since
> that needs the same person. Doing both at once means one IT request rather than
> two, and rotating a key you are about to retire is wasted effort.

**Who runs this:** someone with IAM admin on the CI project.
**Time:** ~10 minutes, plus the workflow diff in step 9.

WIF replaces the static key with a short-lived credential, minted per run and
scoped to this repository and to specific refs, so there is no long-lived secret
to leak or rotate. See [README.md](README.md) §7 for why this is the target state
and what the current arrangement leaves open.

---

## 0. Before you start

Decision 4 in the spec: reuse the **existing sandbox project** that the old
`GCP_BIGQUERY_USER_KEYFILE` pointed at. Find its ID before running anything —
every command below depends on it:

```bash
gcloud auth login                     # tokens on the dev machine were expired
gcloud projects list                  # identify the sandbox project
```

> **Do not run these against a project holding client GA4 data.** The test suite
> creates and drops datasets. It must be a sandbox.

---

## 1. Set your variables

```bash
export PROJECT_ID="<the-sandbox-project-id>"
export PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"
export REPO="Velir/dbt-ga4"
export POOL="github-actions"
export PROVIDER="dbt-ga4"
export SA_NAME="dbt-ga4-ci"
export SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "project=${PROJECT_ID} number=${PROJECT_NUMBER}"   # sanity check both are set
```

---

## 2. Enable the required APIs

```bash
gcloud services enable \
  iamcredentials.googleapis.com \
  sts.googleapis.com \
  bigquery.googleapis.com \
  --project="${PROJECT_ID}"
```

---

## 3. Create the CI service account

Dedicated to CI, with the **minimum** roles the test suite needs: it creates and
drops datasets (`dataEditor`) and runs query jobs (`jobUser`). Nothing else, and
project-scoped only.

```bash
gcloud iam service-accounts create "${SA_NAME}" \
  --project="${PROJECT_ID}" \
  --display-name="dbt-ga4 CI (Workload Identity Federation)"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.jobUser"
```

> Do **not** create a key for this account (`gcloud iam service-accounts keys
> create`). Needing one means something is misconfigured — the entire point is
> that no static key exists.

---

## 4. Create the Workload Identity Pool and provider

The `--attribute-condition` is the security control. It restricts token minting
to this repository **and** to the refs that are allowed to hold credentials:
`main` and `release-candidate/**`, matching the trust boundary in spec §4. A
fork, a feature branch, or another repo entirely cannot mint a token even if it
somehow reaches this provider.

```bash
gcloud iam workload-identity-pools create "${POOL}" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --display-name="GitHub Actions"

gcloud iam workload-identity-pools providers create-oidc "${PROVIDER}" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="${POOL}" \
  --display-name="dbt-ga4 GitHub Actions" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.ref=assertion.ref" \
  --attribute-condition="assertion.repository == '${REPO}' && (assertion.ref == 'refs/heads/main' || assertion.ref.startsWith('refs/heads/release-candidate/'))"
```

---

## 5. Let the pool impersonate the service account

Binds *only* principals whose `attribute.repository` is this repo. Combined with
the attribute condition above, both the repo and the ref are constrained.

```bash
gcloud iam service-accounts add-iam-policy-binding "${SA_EMAIL}" \
  --project="${PROJECT_ID}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL}/attribute.repository/${REPO}"
```

---

## 6. Add the GitHub repository secrets

Print the provider resource name — this is the exact string the workflows
expect:

```bash
echo "GCP_WORKLOAD_IDENTITY_PROVIDER=projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL}/providers/${PROVIDER}"
echo "GCP_SERVICE_ACCOUNT=${SA_EMAIL}"
echo "BIGQUERY_PROJECT=${PROJECT_ID}"
```

Add all three under **Settings → Secrets and variables → Actions**:

| Secret | Value | Sensitive? |
|---|---|---|
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | the `projects/.../providers/...` string above | No, but keep it a secret by convention |
| `GCP_SERVICE_ACCOUNT` | `dbt-ga4-ci@<project>.iam.gserviceaccount.com` | No |
| `BIGQUERY_PROJECT` | the sandbox project ID | No |

None of these are credentials — they are identifiers. The credential is minted
per-run from the OIDC token, which is the whole design.

---

## 7. Switch the workflows to WIF

Until this step lands, the workflows still use `credentials_json` and WIF is
inert. In `main.yml` (and `release.yml` once it exists), replace:

```yaml
      - name: Authenticate to GCP
        uses: google-github-actions/auth@7c6bc770dae815cd3e89ee6cdf493a5fab2cc093 # v3.0.0
        with:
          credentials_json: ${{ secrets.GCP_BIGQUERY_USER_KEYFILE }}
```

with:

```yaml
      - name: Authenticate to GCP via Workload Identity Federation
        uses: google-github-actions/auth@7c6bc770dae815cd3e89ee6cdf493a5fab2cc093 # v3.0.0
        with:
          workload_identity_provider: ${{ secrets.GCP_WORKLOAD_IDENTITY_PROVIDER }}
          service_account: ${{ secrets.GCP_SERVICE_ACCOUNT }}
```

and re-add `id-token: write` to that job's `permissions:` block — **job-scoped,
never at workflow scope**. Nothing else changes: `conftest.py`, the scripts and
the tests all resolve credentials through ADC and do not care which kind was
minted.

## 8. Retire the old key

Only after a WIF-authenticated run is green:

```bash
# List keys on whatever SA the old workflow used, then delete the USER_MANAGED one.
gcloud iam service-accounts keys list --iam-account="<old-sa-email>" --project="${PROJECT_ID}"
gcloud iam service-accounts keys delete "<KEY_ID>" --iam-account="<old-sa-email>"
```

Then delete the `GCP_BIGQUERY_USER_KEYFILE` GitHub secret. This supersedes the
rotation task in [README.md](README.md) §1.1 — a key you have retired does not
need rotating.

---

## 9. Verify

Trigger Tier 2 manually (**Actions → Tier 2 → Run workflow**) rather than
waiting for a merge. Expect:

1. `checks` passes — no credentials involved, so this should behave exactly as
   it does on a PR.
2. `integration (bigquery)` reaches the auth step and prints
   `Created credentials file at ...`.
3. The test suite runs. **This is the first time these tests have ever executed
   in CI**, and the first time they have run at all since the pytest 9 breakage,
   so treat a failure here as "possibly a real test bug", not necessarily an
   auth problem.

### If auth fails

| Symptom | Likely cause |
|---|---|
| `Permission denied on resource ... workloadIdentityPools` | The `principalSet` binding in step 5 doesn't match — check `PROJECT_NUMBER` is the **number**, not the ID |
| `The given credential is rejected by the attribute condition` | Running on a ref other than `main` / `release-candidate/**`. Expected; that is the control working |
| `Unable to acquire impersonated credentials` | Step 5's `workloadIdentityUser` binding missing or on the wrong SA |
| `Access Denied: Project ...: User does not have bigquery.jobs.create` | Step 3's role bindings did not apply |
| dbt cannot find credentials at all | The `id-token: write` permission is missing from the job, or the auth step ran after `test.sh` |
