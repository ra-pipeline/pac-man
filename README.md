# 📡⚡ PAC-MAN (Pipeline Asynchronous Computing Manager)

An external orchestrator package designed to hook into the core [CASA](https://casa.nrao.edu/)-based `pipeline` framework (`tier0`) and distribute tasks across local worker processes or [HTCondor](https://htcondor.org/)/[OSG](https://osg-htc.org/) using [Parsl](https://parsl-project.org/).

Documentation is available at [https://ra-pipeline.github.io/pac-man/](https://ra-pipeline.github.io/pac-man/).

## Design Concept

Instead of rewriting the core pipeline, `pac-man` acts as an "Executor Plugin":
1. The core pipeline gathers its workflow stages from the PPR.
2. The core pipeline delegates the execution of subtasks to an abstract "Executor".
3. `pac-man` registers itself as that Executor, wraps the subtasks in Parsl `@python_app` decorators, and dispatches them natively to compute pools.

## Setup

```bash
# Initialize the environment with Pixi
pixi install

# Run tests
pixi run test

# Run pipeline reduction test (automatically isolated in working/)
pixi run pipeline-test --telescope vla
```

## References & Citations

- **Parsl**: Babuji, Y., et al. (2019). "Parsl: Pervasive Parallel Programming in Python". In *Proceedings of HPDC '19*, pp. 25–36. DOI: [10.1145/3307681.3325400](https://doi.org/10.1145/3307681.3325400)
- **CASA**: CASA Team, et al. (2022). "CASA, the Common Astronomy Software Applications for Radio Astronomy". *PASP*, 134(1041), 114501. DOI: [10.1088/1538-3873/acac61](https://doi.org/10.1088/1538-3873/acac61)
- **HTCondor**: Thain, D., Tannenbaum, T., & Livny, M. (2005). "Distributed computing in practice: the Condor experience". *Concurrency and Computation: Practice and Experience*, 17(2–4), 323–356. DOI: [10.1002/cpe.938](https://doi.org/10.1002/cpe.938)

