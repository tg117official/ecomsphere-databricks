# EcomSphere — Session 17 Databricks lab

Deployable, serverless notebook-task training slice with six relational source tables and one JSON event feed. The job initializes the lab namespace, generates three deterministic batches, uses Delta query-based CDC and Auto Loader with an AvailableNow trigger, validates and models sales, reconciles and commits watermarks. No external cloud account is required.

The full schema and DQ catalog in `config/` describe the wider EcomSphere project. They are reference contracts, not a claim that all 56 tables and 50 rules are implemented. This lab uses reduced Silver/Gold projections, selected DQ classifications and one customer SCD2 dimension. No production JDBC, real ADLS, full rule engine, wheel or CD workflow is included yet.

Start with the accompanying student guide. Use a dedicated catalog/prefix; run batches 1, 2, 3 serially. Never run the reset job while the pipeline is active.

Local fixture checks: `python -m unittest discover -s tests -v`.
