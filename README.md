This repository contains the model code used in the submitted paper:
Integrated modelling to support analysis of COVID-19 impacts on London's water system and in-river water quality

Barnaby Dobson<sup>1</sup>, Tijana Jovanovic<sup>2</sup>, Yuting Chen<sup>1</sup>, Athanasios Paschalis<sup>1</sup>, Adrian Butler<sup>1</sup>, Ana Mijic<sup>1</sup>

<sup>1</sup>Department of Civil and Environmental Engineering, Imperial College London, London, UK
<sup>2</sup>British Geological Survey, Nottingham, UK

Contact details: b.dobson-at-imperial.ac.uk


# CityWatSemiDist (CWSD)
A Modular Semi-Distributed Water Management Model.

## Repository structure
```
|---- data
|    |---- raw
|    |---- processed
|    |---- results
|---- scripts
|    |---- preprocessing
|    |---- orchestration
|    |---- postprocessing
|---- workflow.txt
```

Instructions for applying CWSD can be found in workflow.txt

## Requirements
cwsd has been developed using Python 3.7.6. 
This is most easily installed with Anaconda by following instructions at: https://docs.anaconda.com/anaconda/install/.

The following Python modules are used:
 - numpy
 - pandas
 - tqdm
 - matplotlib
 - geopandas
 - os
 - sys
 - datetime
 - scipy
 - geopandas
 - ukcensusapi
