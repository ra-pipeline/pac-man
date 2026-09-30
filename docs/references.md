# References & Citations

This page indexes academic publications, official documentation, and technical resources relevant to PAC-MAN, Parsl, CASA, and high-throughput workload managers.

## Academic Citations

### Parsl
If you use PAC-MAN or Parsl in academic research, please cite the primary Parsl paper:

- **Paper**: Babuji, Y., Woodard, A., Li, Z., Katz, D. S., Clifford, B., Kumar, R., Lacinski, L., Chard, R., Woitaszek, M., Foster, I., Wilde, M., & Chard, K. (2019). "Parsl: Pervasive Parallel Programming in Python". In *Proceedings of the 28th International ACM Symposium on High-Performance Parallel and Distributed Computing (HPDC '19)*, pp. 25–36.
- **DOI**: [10.1145/3307681.3325400](https://doi.org/10.1145/3307681.3325400)
- **BibTeX**:
  ```bibtex
  @inproceedings{babuji2019parsl,
    author    = {Babuji, Yadu and Woodard, Anna and Li, Zhuozhao and Katz, Daniel S. and Clifford, Ben and Kumar, Rohan and Lacinski, Lukasz and Chard, Ryan and Woitaszek, Michael and Foster, Ian and Wilde, Michael and Chard, Kyle},
    title     = {Parsl: Pervasive Parallel Programming in Python},
    year      = {2019},
    isbn      = {9781450366700},
    publisher = {Association for Computing Machinery},
    address   = {New York, NY, USA},
    url       = {https://doi.org/10.1145/3307681.3325400},
    doi       = {10.1145/3307681.3325400},
    booktitle = {Proceedings of the 28th International ACM Symposium on High-Performance Parallel and Distributed Computing},
    pages     = {25--36},
    numpages  = {12},
    location  = {Phoenix, AZ, USA},
    series    = {HPDC '19}
  }
  ```

### CASA (Common Astronomy Software Applications)
For pipeline calibration and imaging routines:

- **Paper**: CASA Team, Bean, R., Bhatnagar, S., Castro, S., Donovan Meyer, J., Emonts, B., Garcia, E., Golap, K., Gonzalez, J., Jagannathan, P., et al. (2022). "CASA, the Common Astronomy Software Applications for Radio Astronomy". *Publications of the Astronomical Society of the Pacific*, 134(1041), 114501.
- **DOI**: [10.1088/1538-3873/acac61](https://doi.org/10.1088/1538-3873/acac61)
- **BibTeX**:
  ```bibtex
  @article{casa2022,
    author    = {{CASA Team} and Bean, R. and Bhatnagar, S. and Castro, S. and {Donovan Meyer}, J. and Emonts, B. and Garcia, E. and Golap, K. and Gonzalez, J. and Jagannathan, P. and others},
    title     = {CASA, the Common Astronomy Software Applications for Radio Astronomy},
    journal   = {Publications of the Astronomical Society of the Pacific},
    volume    = {134},
    number    = {1041},
    pages     = {114501},
    year      = {2022},
    doi       = {10.1088/1538-3873/acac61},
    url       = {https://doi.org/10.1088/1538-3873/acac61}
  }
  ```

### HTCondor
For cluster batch scheduling and distributed worker execution:

- **Paper**: Thain, D., Tannenbaum, T., & Livny, M. (2005). "Distributed computing in practice: the Condor experience". *Concurrency and Computation: Practice and Experience*, 17(2–4), 323–356.
- **DOI**: [10.1002/cpe.938](https://doi.org/10.1002/cpe.938)
- **BibTeX**:
  ```bibtex
  @article{thain2005condor,
    author    = {Thain, Douglas and Tannenbaum, Todd and Livny, Miron},
    title     = {Distributed computing in practice: the Condor experience},
    journal   = {Concurrency and Computation: Practice and Experience},
    volume    = {17},
    number    = {2-4},
    pages     = {323--356},
    year      = {2005},
    doi       = {10.1002/cpe.938},
    url       = {https://doi.org/10.1002/cpe.938}
  }
  ```

### Slurm Workload Manager
For HPC partition resource scheduling:

- **Paper**: Yoo, A. B., Jette, M. A., & Grondona, M. (2003). "SLURM: Simple Linux Utility for Resource Management". In *Job Scheduling Strategies for Parallel Processing (JSSPP 2003)*, Lecture Notes in Computer Science, vol 2862, pp. 44–60. Springer, Berlin, Heidelberg.
- **DOI**: [10.1007/10968980_3](https://doi.org/10.1007/10968980_3)
- **BibTeX**:
  ```bibtex
  @inproceedings{yoo2003slurm,
    author    = {Yoo, Andy B. and Jette, Mark A. and Grondona, Mark},
    title     = {{SLURM}: Simple Linux Utility for Resource Management},
    booktitle = {Job Scheduling Strategies for Parallel Processing},
    series    = {Lecture Notes in Computer Science},
    volume    = {2862},
    pages     = {44--60},
    publisher = {Springer},
    year      = {2003},
    doi       = {10.1007/10968980_3},
    url       = {https://doi.org/10.1007/10968980_3}
  }
  ```

---

## Official Documentation & Project Links

### Workflow & Execution Engines
- [Parsl Project Homepage](https://parsl-project.org/)
- [Parsl Documentation](https://parsl.readthedocs.io/)
- [Parsl HighThroughputExecutor Reference](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html)
- [Parsl Workflow Construction & DataFlowKernel](https://parsl.readthedocs.io/en/stable/userguide/workflows.html)
- [Parsl Monitoring & Visualization](https://parsl.readthedocs.io/en/stable/userguide/monitoring.html)

### Astronomy Software & Pipelines
- [CASA Homepage](https://casa.nrao.edu/)
- [CASAdocs Official Documentation](https://casadocs.readthedocs.io/)
- [ALMA & VLA Science Pipeline Overview](https://almascience.nrao.edu/processing/science-pipeline)
- [CASA Memo Series](https://casadocs.readthedocs.io/en/stable/notebooks/memo-series.html)

### Cluster Schedulers & Grids
- [HTCondor Project](https://htcondor.org/)
- [HTCondor Manual](https://htcondor.readthedocs.io/)
- [Open Science Grid (OSG)](https://osg-htc.org/)
- [Slurm Workload Manager](https://slurm.schedmd.com/)

### Tooling & Telemetry
- [Pixi Package Manager](https://pixi.sh/)
- [Streamlit Dashboard Framework](https://streamlit.io/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [PAC-MAN GitHub Repository](https://github.com/ra-pipeline/pac-man)
