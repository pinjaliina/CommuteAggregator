# CommuteAggregator
An application to calculate aggregated travel times based on Helsinki Region Travel Time Matrix (HRTTM)

This application calculates aggregated travel times as reported by the HRTTM. The actual commutes that are to be aggregated are taken from the Finnish Monitoring System of Spatial Structure and Urban Form (MSSSUF or just SSUF; fi: YKR).

Before you can execute the application, you need a database with suitable data and data structure; for the preparation of such a DB, see PREPARATION.md.

You also need the datasets. [The SSUF dataset is not open and not free](https://www.ymparisto.fi/en-US/Living_environment_and_planning/Community_structure/Information_about_the_community_structure/Delineation_of_densely_populated_areas/Delineation_of_Localities_Densely_Popula(26836)), but Finnish higher education institutions do have access. For the TTM, see the following:
* a data description by Tenkanen & Toivonen (2020): https://doi.org/10.1038/s41597-020-0413-y
* the code used to create the most recent version of the matrix: https://github.com/AccessibilityRG/HelsinkiRegionTravelTimeMatrix2018
* the actual data: https://blogs.helsinki.fi/accessibility/data/
