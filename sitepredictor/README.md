# Predicting wind turbine success

## Stefan Haselwimmer, May 2025

The planning constraint layers underlying Open Wind Energy are based on the Centre for Sustainable Energy's ["Identifying suitable areas for onshore wind development"](https://www.cse.org.uk/resource/identifing-suitable-areas-for-onshore-wind-development-in-your-neighbourhood-plan/). However these constraint layers provide a highly risk-averse approach to wind turbine siting and make little distinction between constraints that may have marginal impact on a project's likely success versus layers that have significant impact. 

There are also additional factors beyond proximity to geospatial features - such as local political affiliation or demographics - that could potentially affect a project's likely success. It was therefore decided that a comprehensive investigation of historical data around wind turbine positions could be used to create a more continuous probabilistic 'heatmap' of wind turbine siting. 

This investigation consisted of the following phases:

1. Build high-quality database of failed and successful wind turbine positions

The [Renewable Energy Planning Database (REPD)] (https://www.gov.uk/government/publications/renewable-energy-planning-database-monthly-extract) currently only provides a single geographical coordinate for each project. While this is acceptable for single turbine projects, larger wind farms may consist of multiple turbine positions and an 'averaged' single position risks blurring or misrepresenting crucial geospatial information. For example, the averaged position of a large non-symmetrical wind farm may violate a number of 'essential' planning constraints even if none of the constituent wind turbines do.

It was therefore concluded that a manual collation of all* failed and successful wind turbine positions - rather than the average position of each project - was necessary to maximise the accuracy of any analysis. It was envisioned this would be carried out using planning data publicly available from local authority websites and other internet sources. A bespoke data entry system was built that allowed fast and accurate manual entry of turbine positions, and also 'manual tracing' of site plans when no explicit turbine coordinates were available:



This manual collation work was carried out between October 2024 and February 2025 for all REPD onshore wind projects up to the end of 2024. The manual collation generated a total of [TO COME] wind turbine positions for those projects that failed to go through to construction; for constructed projects, GIS data of wind turbines can be trivially obtained from [OpenStreetMap](https://www.openstreetmap.org/). The final data for failed and successful wind turbines can be downloaded from ["Final and successful wind turbine positions"](https://data.openwind.energy/dataset/failed-and-successful-wind-turbine-positions).

* In most instances, precise wind turbine coordinates could be obtained from planning application documents, including statutory consultee responses. In some instances it was necessary to 'manually trace' visual maps to derive wind turbine positions. On rare occasions, it was not possible to obtain wind turbine positions from any publicly available source.

2. Add non-spatial data to model

In addition to analysing the proximity of a wind turbine to geographical or heritage features like National Parks and Conservation Areas, it was also clear other 'non-physical' location-based attributes could potentially affect the likely success of a wind turbine project, eg. the political affiliation or housing tenure of people close to a site. 

This work was heavily influenced by [Michael Harper, Ben Anderson, Patrick A.B. James, AbuBakr S. Bahaj, (2019)](https://www.sciencedirect.com/science/article/pii/S0301421519300023) which considered the following local factors:

- Political affiliation of local people near prospective turbine.
- Level of qualification of local people near prospective turbine.
- Property tenure of local people near prospective turbine.
- Age profile of local people near prospective turbine.
- Number of existing and planned turbines near prospective turbine.

In contrast to Harper et al's analysis, we used an advanced machine learning model and so were able to adopt a much finer-grained approach to these attributes. For example, rather than considering the average age of local people, we considered a number of age bands. In addition, our database of actual failed/successful wind turbine position allowed for more accurate assessements of features like 'Number of failed turbines within 10km radius'. 

3. Sentiment analysis

The *public mood* surrounding a specific wind turbine project is potentially a significant factor in the likelihood a project will receive planning permission. We therefore investigated use of ChatGPT to derive **sentiment scores** for each onshore wind project. The specific sentinment score question submitted to ChatGPT was:

```
Returning ONLY a number and no other text, on a scale of -10 (most negative) to +10 (most positive), how positive or negative were all the news articles about "[PROJECT NAME]"?
```

Multiple runs of this sentiment analysis was conducted on all REPD onshore wind projects. However the sentiment scores generated were found to be wildly inconsistent across multiple runs and ChatGPT-derived sentiment scores were therefore removed from the final processing/results for this 'Version 1.0' probability-of-success map. During further iterations of this map, we will implement a fully open-source sentiment analysis methodology to ensure reliability/repeatability of sentiment analysis data and allow user-driven interactive probability maps, ie. users will be able to enter specific parameters and see the map change accordingly.

4. Initial machine learning analysis

After manually collating failed and successful wind turbine positions and gathering related data (political voting records, census data around qualification, tenure, age profile), a comprehensive training set was used to train a machine learning model. This model was then applied to a 20m-resolution grid of points within a small grid square. This generated the following image:


This initial test highlighted an anomaly regarding footpaths and minor roads - namely there is an increased probability of success when a turbine is near to either, which contradict planning advice to position turbines *away* from either. Further investigation of this issue highlighted the problem of *turbine-related footpaths and minor roads*.

## Turbine-related footpaths and minor roads

When a wind turbine or wind farm is constructed, footpaths and/or minor service roads will be constructed to provide specific access to the turbines. However, these footpaths and minor roads will not have existed - and will not have affected - planning assessments during the planning assessment for the scheme. An accurate machine learning model therefore needs to be trained on **historically-accurate datasets**, ie. where turbine-created footpaths and minor roads that did not exist at the time of planning application have been filtered out. 

This filtering was achieved using a simple PostGIS algorithm that identified and then deleted turbine-created footpaths/minor-service-roads. Turbine-created paths were selected as follows:

- The start/end of a path was within 100m of a constructed turbine.
- The path was within 50m of a constructed turbine. 

Using this algorithm it was possible to remove the majority of turbine-created footpaths/minor roads, which was confirmed from manual checking in QGIS. For example, the following image shows 'all footpaths' (blue) and 'turbine-created footpaths/minor roads' (in red) as identified by the algorithm.

5. Moving from low-resolution to high-resolution results

Harper et al.'s (2019) analysis used computationally efficient weighted rasters to produce a final probabilistic raster based on the relative importance of key success factors. However, this approach was not possible for our machine learning approach as each sample point needed to be run individually through the machine learning model. With our machine learning approach, an increase in resolution by 'x' leads to an increase in computational resources by 'x^2'. 

We therefore ran a low-resolution pass with 1km-spaced points to gauge likely resource requirements for building higher-resolution renders:

[Image of 1km render]

This raster took 13 hours to generate on a Silicon M3 Mac with no multiprocessing. Transferring to Amazon's latest Graviton4 processors (4vCPU, 16Gb), a 1km render took hours.

We then added multiprocessing code to the data pipeline and reran the entire process on a xvCPU, yGb server to produce a 20m render; this took days:

We are grateful to Amazon Web Services who generously provided cloud credit to allow this computationally-intensive work to be carried out.

6. Issues with refined probability map

The high resolution probability map revealed that many of the conventional planning constraints did not seem to be relevant to project success. While the aim of our research was to derive the relative importance of specific planning constraint layers to project success, it was surprising to see the lack of relevance of many of these conventional planning layers in the probability map. 

For example, consider the following image which shows a portion of the probability heatmap overlayed with the boundary of the South Downs National Park:

[Image of heatmap with National Park outline overlayed]

Note that as you enter or leave the National Park, there is no sudden decrease/increase in probability, even though National Parks are generally considered to be "no way here" contraint layers.

This lack of conventional planning constraints in our results may, however, be explained with regards to the core dataset used to generate the results - we specifically used failed and successful *planning applications* to train our machine learning model. However, the vast majority of failed and successful planning applications will expend considerable effort to ensure they satisfy planning constraints in general. Our 'failed' category within our training data does not therefore provide a good indication of poor planning constraint satisfaction, even if it may help to inform overall probability-of-success more generally. 

It is necessary to distinguish two separate probabilities that go to make up a project's overall likelihood of success. There is the probability a project will represent a competent planning application:

```
P(site represents competent planning application) [1]
```

There is also a conditional probability that a project will be successful (typically receive planning permission), given it is a competent planning application:

```
P(site will be successful | site represents competent planning application) [2]
```

Our probability heatmap provides location-specific values for this second probability `[2]`. We can use this probability in combination with other probability estimates to provide an overall estimate of **P(site will be successful)**, regardless of whether or not we know if it's 'competent', ie. is the kind of site a wind developer would be confident in submitting to a council for planning permission.

Specifically, the probability a site will be successful will be:

```
(P(site represents competent planning application) * P(site will be successful | site represents competent planning application)) 
+ 
(P(site doesn't represent competent planning application) * P(site will be successful | site doesn't represent competent planning application))
```

If we assume all failed and successful wind turbine planning applications represent 'competent' planning applications, also assume `P(success | not competent) = 0` and finally model what constitutes a 'competent' planning application, we can potentially produce a revised probability map that shows `P(site will be successful)` that might better reflect conventional planning constraints.

One way in which we might attempt to model what constitutes a 'competent' planning application is to consider distances from key planning constraints. For example, if all failed and successful wind turbines are always > 50 metres from National Parks, the probability of a site being competent if within 50 metres of a National Park would be 0. 

Extending this approach to all existing Open Wind Energy planning constraints, we produced the following `P(site represents competent planning application)` probability map:

[Image of site competence probability map]

In combination with our original `P(site will be successful | site represents competent planning application)` probability map, we calculated the product to produce a final `P(success)` probability map:

[Image of final probability map]

Final results were converted into a Cloud Optimized GeoTIFF (COG), which can be viewed at:

The final COG GeoTIFF probability raster can also be downloaded at:








