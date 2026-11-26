# Exercise 17 — Student Assignment: Reverse-engineer EcomSphere

## Objective

Take ownership of an existing data engineering application: reproduce its execution, understand how the source code works, and explain the implementation without rebuilding it.

## Part A — Reproduce the application

Use the student guide and your own Databricks workspace. Deploy the supplied bundle and run batches 1, 2 and 3 in that order. Record the three actual Databricks run IDs, job status, trusted line count, revenue, current rejects and committed watermark. Capture one Jobs DAG screenshot and evidence that the third run corrected the rejected order while retaining its earlier rejection history.

Do not submit an earlier batch after a newer batch has committed. If you need to start over, use the reset job described in the guide; preserve your evidence first.

## Part B — Navigate the code

Open the local repository in an editor. Use these steps for each job task:

1. Start at `databricks.yml`. Follow the `include` into the job definition.
2. Find a task key, its dependency, notebook path and supplied parameters.
3. Open the notebook script. Find the branch that selects the relevant stage.
4. Follow the imported function into the corresponding Python module. Use function definitions, imports and comments rather than guessing from filenames.
5. For each function, note its inputs, operations, decisions, calls to other functions, output tables or files and effects on persistent state. Look up SQL expressions and Spark operations you do not recognize.
6. Follow the next task. Remember that notebook sessions are independent: identify the Delta tables or volumes that bridge them.
7. Compare your findings with actual results from `notebooks/inspect.py`. Mark any behavior that you inferred from code but did not observe in a run.

Use a compact worksheet as you trace:

| Task and file | Function | Reads / inputs | Operation or decision | Writes / outputs | Evidence |
|---|---|---|---|---|---|
| Your finding | Your finding | Your finding | Your finding | Your finding | Query result / run ID |

## Part C — Draw two flowcharts

**Diagram 1: Job-level execution.** Show task dependencies, entry script, major read/write resources, success path and failure path. Start at the runtime parameter and end at completion.

**Diagram 2: Function-level execution.** Show the actual functions within the stages and calls between them. Include branches, loops, decisions, inputs, output datasets and state transitions. Use function names from the source, not only the words Bronze, Silver and Gold. Show where the watermark and checkpoint are used and what happens if reconciliation fails. Highlight the path that rejects O004 and the path that recovers it in batch 3.

A flowchart can be handwritten or prepared with Mermaid, diagrams.net or another diagram editor. Use rectangles for actions, diamonds for conditions, arrows for sequence and cylinders or labels for persisted data. You can split the function-level chart into linked subcharts rather than create one enormous diagram.

## Part D — Ten understanding questions

Answer in your own words after completing the flowcharts. For each answer cite at least one actual filename and function or provide an execution observation; include both when helpful.

| # | Question |
|---:|---|
| 1 | Starting with the bundle, how does a Databricks job task identify and invoke its application function? Explain the difference between `catalog`, `prefix`, `batch`, `stage` and `run_id`. |
| 2 | What does setup create, what does it preserve on a second execution, and what prevents it from adopting an unrelated namespace? How does it detect an out-of-order batch? |
| 3 | What changes in the source between batches 1, 2 and 3? How does the generator avoid duplicating relational source rows or replacing an existing JSON file? |
| 4 | Which control table drives ingestion, what fields does the relational adapter consume, and how does the code select the relational or JSON adapter? Can a required source simply be disabled in this integrated lab? |
| 5 | Calculate the exact relational extraction windows for batches 2 and 3. Why do Bronze counts increase even for unchanged business records, and how is replay of the latest batch protected? |
| 6 | How does the JSON feed ingest only new files? Identify the stream trigger, source path, durable checkpoint and Bronze target. What could happen if only the checkpoint is removed? |
| 7 | Why is O002 accepted at 110.01 while O004 and OI004 are rejected in batch 2? Trace the code path by which batch 3 restores the line while preserving the reject history. |
| 8 | O001 has two payment rows and two shipment rows. Why does it not produce four sales rows? Which customer version is associated with the new orders, and where do the OBT and mart obtain their revenue? |
| 9 | Which checks must succeed before a watermark is advanced? What happens to the pending batch and audit status when a task fails before the commit? What limitation remains when logging fails after the commit? |
| 10 | What would you change to onboard another relational dataset? Identify the source contract, fixture, setup/configuration, adapter constraints, downstream transformation and verification changes. Explain why adding a metadata row alone is insufficient here. |

## Submission and review

Submit two diagrams, a short answer document (questions 1–10) and three-run evidence. Distinguish observed results from proposed or inferred behavior. Do not include passwords, access tokens or private workspace information in shared screenshots. The review will focus on correct execution flow, function responsibilities, inputs/outputs, decision logic and state management rather than diagram decoration.
