# Characterizing the effect of retractions on scientific publishing careers

[![DOI](https://zenodo.org/badge/634166007.svg)](https://zenodo.org/badge/latestdoi/634166007)


This repository contains data and code created for the study on "Characterizing the effect of retractions on scientific publihsing careers."

- **code/** folder contains all the code used to create the main and supplementary analysis, tables, and plots.

- **plot_data/** folder contains all the data used to create the plots and tables. 
	- Note1: The data file for 9.altmetric_exploration_plots_altmetric_api_rematching.ipynb is not provided as our data user agreement with Altmetric prevents us from sharing this data.
	- Note2: The data file for 5.collaborator_geo_analysis.ipynb is not provided as it is > 2gb in size and github has a data limitation of 50 MB. However, this file is available on request.

- **plots/** folder contains all the plots that are part of our paper.

# System Requirements
## Hardware requirements
The code provided is for reproducing the plots and tables related to the quantitative results and analysis in the paper. It requires only a standard computer or laptop with enough RAM to support the in-memory operations.
This code was tested on the MacBook Pro (16-inch, 2019) with memory 32 GB 2667 MHz DDR4 and processor 2.4 GHz 8-Core Intel Core i9.

## Software requirements
### OS Requirements
The code should work on any operating system as long as Python, R, and Jupyter are installed. This was tested on macOS Monterey.

### Python Dependencies
```
pandas==2.0.3
matplotlib==3.4.1
Cython==0.29.32
seaborn==0.11.0
scipy==1.6.3
numpy==1.20.3
geopy==2.4.1
ptitprince==0.2.7
```

### R Dependencies

```
library("survival")
library("survminer")
```

# Installation Guide:

All packages can be installed using Pypi

# Demo

This code simply reproduces all the figures and tables in the paper. All the code is in the form of notebooks. For the cells, the output tables and plots are shown in the notebooks.

## Contributors
- [Shahan Ali Memon](samemon@uw.edu)
- [Bedoor AlShebli](bedoor@nyu.edu)
- [Kinga Makovi](km2357@nyu.edu)

## Acknowledgements
I express my sincere gratitude to my advisors, [Kinga R. Makovi](km2537@nyu.edu) and [Bedoor AlShebli](bedoor@nyu.edu), whose insightful guidance and mentorship have been invaluable throughout the course of this project. Their expertise and support have played a pivotal role in shaping and enriching my academic journey.

