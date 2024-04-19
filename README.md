## Primary Image for python requirements: https://github.com/konzainc/konza-kube/blob/main/docker/airflow/Dockerfile
## Virtual Environment image to be used as minimally as possible and may be rejected in code reviews

Airflow development is best done via Bastion hosts, which should have access to all storage accounts and databases that the prod environment also has access to. This means that any connectivity / security issues are assumed to be resolved for the Bastion host to the same extent as they are used to being resolved for the Airflow Kubernetes workers.

Each developer should be allocated their own Bastion host, which they can use for development. These bastion hosts can be paused when not in use however. Note that bastion hosts can also serve as a remote backend for an IDE, for developers who like to use IDEs rather than vim or emacs.

Testing an Airflow DAG is typically a multi-stage process. What tests will look like for the DAG depend on a number of factors, e.g.:
- nature of the task at hand
- characteristics and size of the data
- requirements re: reproducibility and explainability
- level of project maturity

In mature data pipelines there will typically three levels of testing:
- unit / integration testing for library code
- DAG local testing
- DAG remote testing

## Library code testing

Oftentimes an Airflow data pipeline begins life as a set of disparate Python functions, which Airflow will stitch together. Before writing the Airflow DAG it makes sense to consider writing tests that verify the functionality of various bits of individual code. This is best done using a framework like `pytest`, and does not really involve Airflow itself.

Good, trustworthy DAGs will feature thoroughly unit-tested "business-logic" functions -- it is typically much easier to debug a broken function using a unit test than it is in the context of an Airflow DAG.

Functions that comprise Airflow DAGs will often need to interact with remote databases and APIs. To verify their functionality it is often desirable to write both unit tests (which rely on "mocked" versions of remote services) and integration tests -- more sophisticated tests which rely on functional test-focused replicas of real services.

Good tests need to reproduce production conditions and this often means interacting with realistic data. Developers should take care to produce realistic test cases that nonetheless do not (!) include any PII or PHI. It is thus desirable to use synthetic data generators such as Synthea for instance.

## DAG local testing

A second level of testing involves ascertaining an entire DAG works when run from the Bastion host. This can be done locally, without the need to run an Airflow scheduler if following [this pattern](https://docs.astronomer.io/learn/testing-airflow?tab=decorator#debug-interactively-with-dagtest) when writing a DAG. DAGs written this way can also be run directly using `python [path to DAG]` from the Bastion host.

It is important to note that the DAG in this case runs as a single Python process, and its resourcing is limited to whatever resources are available on the Bastion host. This does mean that a local DAG test may not look the same as a production DAG run. For instance, the local DAG test could use fewer data. Developers should be encouraged to use [Airflow Params](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html) to set up their DAGs, so as to enable the easy execution of local test runs.

## Local testing using Docker compose

This is a very effective way to test DAGs by setting up a fully-fledged Airflow server on your local machine. 

A few things to note:
- any DAGs that require access to remote services will need to have that access set up using some mechanism. For instance, if you expect to have access to an Azure Blob Storage account using a mount, that mount will need to be available (1) on the machine on which you run `docker compose` and (2) inside the `airflow-worker` image on `docker-compose`.
- local airflow uses the `CeleryExecutor`, which is slightly different from `KubernetesExecutor`. This may create issues if testing DAGs which rely on tight integration with Kubernetes, e.g. via `PodOverride`, etc. Task execution via `KubernetesOperator` is not affected by this however.
- any tasks using regular `PythonOperator`, `BashOperator`, `PythonVirtualEnvOperator` etc. should work the same way in local testing and remotely.
- this workflow has only been tested on Linux machines, some small changes may be necessary on Windows.

To try out local testing using docker compose:
- Clone this and the (private) `ccd-parse` repos.
- Edit the `.env` file included in this repository to reflect the local location of your `ccd-parse` clone and your local `.xml` files.
- If using the example `.xml` files in the `ccda` directory of the `ccd-parse` repo, you will have to grant airflow read access to this directory, e.g. using `chmod 704` on Linux.
- Install Docker Engine if not already installed (try `docker compose` in cli): https://docs.docker.com/engine/install/ubuntu/
- `cd` into the root of this repository and enter `docker compose up`. `sudo` may be neccessary for all docker commands.
- Wait for necessary services to start (can check with `docker container ps -a` for success).
- In the browser type in `http://localhost:8080` to access the airflow web UI.
- Login using username: `airflow`, password: `airflow`.
>>> NOTE: 2 dags currently fail to import: `sftp_board_log_retrieval` and `board_shallow_copy`. This is expected at this stage.

You should see the following image:

![Airflow DAGs](docs/dags.png)

- Activate (pressing the toggle button next to the DAG) and run the desired DAG from the UI. `test_basic_extract` is provided as an example for CCD parsing.
![Airflow DAG Run](docs/dag-run.png)

- To stop airflow use `docker compose down`.

## DAG remote testing

Both library code and DAG local testing can happen independently of the source control process, and it is to be expected that a developer would run tests many times before reaching code they would like to submit for review. A best-practice level of validation for the functionality of the code submitted in the PR involves running the new (or modified) DAG in an environment that is akin to production, but that is decidedly NOT the production environment into which the developer is trying to merge their code (the purpose of the test being to ascertain the code is ready for production).

DAG remote testing thus allows the developer to test a commit pushed to a github branch in the `konza-dags` repository in an environment that faithfully reproduces production.

This environment is easiest to implement as a replica environment to the airflow production environment, using the source branch for the PR being tested as the source code. Any data artifacts coming out of the environment should be marked as being test (rather than prod) artifacts, since their data lineage will not be trustworthy. [Airflow templates](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html) can be used to achieve this requirement.

The replica test environment(s) should be accessible in the same way as the prod environment. Successful DAG runs from a test environment should be appended to DAG PRs as proof the code works as expected -- this can also help trace back why a certain set of modifications to a pipeline was approved if questions arise later on.

An important question is what to do about multiple developers needing to test their code at the same time. In a small organization it is perfectly acceptable to have a single remote environment which developers share (e.g. via an MS Teams channel in which they manually lock and unlock the environment as needed). Switching branch under test in the environment can be done by editing a single environment variable indicating the github branch being pulled in a git-sync container. As the team grows, multiple environments (one per developer) will become necessary, though this will typically require several data engineers actively developing DAGs.

## Maturity cycle for libraries

Developers are free to use operators such as `PythonVirtualEnvOperator` to install or upgrade Python libraries needed for specific code, as long as they are executed against the same base image.

> Note: exceptions from this rule of using the same base image may be acceptable, but only for special and well-argued cases, e.g. needing CUDA bindings for an ML application.

If a library is used often (either in many tasks or in a task that needs to run many times), libraries installed in a `PythonVirtualEnvOperator` should be pushed inside the Docker image.
