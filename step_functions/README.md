# Step Functions — Pipeline Orchestrator

The state machine in `state_machine.json` is the brain of the pipeline. It sequences every step, handles retries, waits for dbt to actually finish, and sends a success or failure email at the end.

---

## State machine flow

```
GenerateAndUpload
      ↓
LoadToStaging
      ↓
RunDbt (sendCommand to EC2)
      ↓
WaitForDbt (30s pause)
      ↓
CheckDbtStatus (poll SSM)
      ↓
IsDbtComplete?
  ├── Still running  → back to WaitForDbt
  ├── Success        → NotifySuccess → PipelineComplete ✓
  └── Failed/TimedOut/Cancelled → NotifyFailure → PipelineFailed ✗
```

---

## States

### GenerateAndUpload
**Type:** Lambda invoke

Calls `retail-fmcg-lambda-1-generator`. The full event payload (`$`) is forwarded as-is, so a manual run with an explicit `run_date` passes straight through to Lambda 1. The result is stored at `$.lambda1_result`.

**Retry:** 2 attempts, 30s initial wait, 2× backoff.

---

### LoadToStaging
**Type:** Lambda invoke

Calls `retail-fmcg-lambda-2-staging`. Receives `$.lambda1_result.Payload` — this is how Lambda 2 gets the S3 URIs that Lambda 1 just uploaded, with no manual wiring between them. The result is stored at `$.lambda2_result`.

**Retry:** 2 attempts, 30s initial wait, 2× backoff.

---

### RunDbt
**Type:** SSM SendCommand

Sends a shell script to EC2 via SSM. This call returns immediately when SSM **accepts** the command — it does not wait for dbt to finish. That's why `WaitForDbt` and `CheckDbtStatus` exist.

**Commands sent to EC2:**
```bash
export PATH=/home/ec2-user/.local/bin:$PATH
export PYTHONPATH=/home/ec2-user/.local/lib/python3.9/site-packages:$PYTHONPATH
cd /home/ec2-user/dbt/fmcg_pipeline
export DB_HOST=$(aws ssm get-parameter --name /fmcg/db_host --with-decryption ...)
export DB_USER=$(aws ssm get-parameter --name /fmcg/db_user --with-decryption ...)
export DB_PASSWORD=$(aws ssm get-parameter --name /fmcg/db_password --with-decryption ...)
dbt run --select staging   # build stg_* views
dbt snapshot               # SCD Type 2 on products and stores
dbt run --select gold      # build fact and dim tables
dbt test                   # validate data quality
```

**Why `PATH` and `PYTHONPATH`:** SSM runs as `root` and doesn't load the `ec2-user` shell profile. dbt is installed under `ec2-user`'s local Python packages, so both must be set explicitly.

**Why `--with-decryption`:** The SSM parameters are `SecureString` (KMS encrypted). Without this flag, the raw ciphertext is returned instead of the actual value.

**CloudWatch output:** dbt stdout and stderr are streamed to `/fmcg/ssm/dbt` for debugging.

The result (`$.dbt_result`) contains the SSM `CommandId` used by the polling loop.

---

### WaitForDbt
**Type:** Wait

Pauses 30 seconds before polling SSM. This prevents hammering the `GetCommandInvocation` API while dbt is still starting up.

---

### CheckDbtStatus
**Type:** SSM GetCommandInvocation

Polls SSM using the `CommandId` from `$.dbt_result.Command.CommandId` to check whether the dbt script has finished. The result is stored at `$.dbt_status`.

If SSM hasn't registered the invocation yet (can happen in the first poll), the Catch sends execution back to `WaitForDbt` to try again.

---

### IsDbtComplete
**Type:** Choice

Routes based on `$.dbt_status.Status`:

| Status | Next state |
|---|---|
| `Success` | NotifySuccess |
| `Failed` | NotifyFailure |
| `TimedOut` | NotifyFailure |
| `Cancelled` | NotifyFailure |
| Anything else (`Pending`, `InProgress`) | WaitForDbt (loop) |

---

### NotifySuccess
**Type:** SNS Publish

Sends a success email to the SNS topic. The message is built dynamically:
```
Pipeline completed successfully for 2026-04-25. Rows loaded: 760
```

---

### NotifyFailure
**Type:** SNS Publish

Sends a failure email containing the full Step Functions state as JSON — includes the exact error, which Lambda or dbt command failed, and any stack trace captured by SSM.

---

### PipelineComplete / PipelineFailed
Terminal states. `PipelineComplete` marks the execution as **Succeeded**. `PipelineFailed` marks it as **Failed** with cause `"One or more pipeline steps failed"`.

---

## Data flow between states

Lambda 1's full output payload lives at `$.lambda1_result.Payload` and is passed directly as the input to Lambda 2. This is how Lambda 2 receives the S3 URIs without any hardcoding — the state machine wires them together automatically.

```
EventBridge input: {}
  ↓
Lambda 1 output: { run_date, run_timestamp, s3_uris, rows_generated }
  stored at: $.lambda1_result.Payload
  ↓
Lambda 2 input: $.lambda1_result.Payload   ← directly forwarded
Lambda 2 output: { status, run_id, run_date, rows_loaded }
  stored at: $.lambda2_result.Payload
  ↓
NotifySuccess reads: $.lambda1_result.Payload.run_date
                     $.lambda2_result.Payload.rows_loaded
```

---

## Deploying

The state machine is deployed manually via the AWS Console:

1. Go to **Step Functions → State machines**
2. Select `retail-fmcg-daily-pipeline` → **Edit**
3. Paste the contents of `state_machine.json`
4. Click **Save**

After any change to `state_machine.json`, redeploy using the steps above.

---

## Manual trigger / backfill

Go to **Step Functions → Start execution** and pass:

```json
{
  "run_date": "2026-04-25",
  "run_timestamp": "2026-04-25 06:00:00"
}
```

For a fully automated daily run, pass `{}` — Lambda 1 defaults to today's date in AEST.

---

## Debugging a failed run

1. Open the failed execution in the AWS Console
2. Click **CheckDbtStatus** in the graph → **Output** tab
3. Look at `$.dbt_status.StandardErrorContent` and `$.dbt_status.StandardOutputContent` for the exact dbt error
4. Or check **CloudWatch → Log groups → `/fmcg/ssm/dbt`** for the full dbt output with line-by-line detail

---

## EC2 instance

SSM commands target instance `i-0d5a310331e078521`. If the EC2 instance is replaced, update `InstanceIds` in both the `RunDbt` and `CheckDbtStatus` states and redeploy the state machine.
