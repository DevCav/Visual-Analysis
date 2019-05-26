*******************************************************************************************
DESCRIPTION********************************************************************************
*******************************************************************************************
This document contains instructions to run CharIOT, as well as instructions to build it from the ground up.

CharIOT is a tool to help individuals, organizations, and governments interact more effectively with the ecosystem of charitable nonprofit organizations in the United States by showing the extent of, characteristics of, and changes in the flow of grant dollars from one part of the country to another.

CharIOT has a number of interesting features:
	-Interactive choropleth map for the user to select and view nonprofit financial flows between parts of the U.S.
	-Interactive node graph view for the user to be able to more easily identify financially important map areas that might be small in area, with Gaussian mixture model-generated clusters to highlight socioeconomically interesting geographies
	-Ability to explore sub-networks of financial flows between geographic areas using groups formed from Latent Dirichlet Analysis on selected text fields from nonprofit tax filings
	-Model-generated values of average simultaneous grant-conditional financial flows

Datasets used here come from:
	IRS 990 Filings on AWS (https://docs.opendata.aws/irs-990/readme.html)
	American FactFinder (https://factfinder.census.gov/faces/tableservices/jsf/pages/productview.xhtml?pid=BP_2016_00CZ1&prodType=table)
	Shapefiles from 
		jefffriesen: (https://gist.github.com/jefffriesen/6892860)
		Chris Given: (https://bl.ocks.org/cmgiven/d39ec773c4f063a463137748097ff52f)

*******************************************************************************************
INSTALLATION*******************************************************************************
*******************************************************************************************
A full dataset build requires:
	-R 3.5.1
	-Python 3.7
	-Unix-like OS
	
The following instructions will produce a small and likely uninteresting toy dataset with 2000-3000 organizations per year. The full dataset, which is made available as part of this package already, does not need to be generated. The code to generate the full dataset anyway can be made possible by changing BOTH instances of the following line in src/download990_faster.R from:
	max=2000
to:
	max=nrow(index_file)-nrow(index_file)%%increment

Warning! The full edition of this scraping code is extremely computationally intensive as a result of extensive I/O operations and XML tree searches. It took several hours to complete running on over 100 cores in a high performance computing environment, and is not recommended to be run in full on a consumer-grade laptop. The reviewer is encouraged to view the code, but to use the produced datasets instead of attempting to fully regenerate them himself or herself. (The provided code and instructions below to make a toy dataset of a few thousand tax records should not pose a problem, though the quality of the LDA analysis and pairwise association mining will be poor without the full sample.)

1. Change directory to generate-irs-dataframe.
2. In the src/ directory of the extracted folder, set the parameter passed to the setwd() function in the first line of download990_faster.R to the absolute path containing the src/ folder (in double quotes).
3. In a command line, switch to the directory containing the src/ folder, and run the following commands:
	Rscript src/download990_faster.R 2011 990
	Rscript src/download990_faster.R 2012 990
	Rscript src/download990_faster.R 2013 990
	Rscript src/download990_faster.R 2014 990
	Rscript src/download990_faster.R 2015 990
	Rscript src/download990_faster.R 2016 990
4. After the above scripts have completed running, run the following commands in the command line:
	python3 out/data/merge_index_table.py
	python3 out/data/merge_recipient_table.py
	python3 out/data/topic_lda_final_version.py
	python3 src/cluster.py
	python3 src/marketbasket.py
5. Finally, take the following files in out/data/ and move them to the geolinks/ directory, where geolinks.html and the other .js files live.
	index_agg_grant_flow.csv
	index_agg_grant_group_flow.csv
	synth_2016.csv
	zip_cluster.csv

*******************************************************************************************
EXECUTION**********************************************************************************
*******************************************************************************************

View an instructional video here! https://youtu.be/j4Ujn4acYLQ

Using the CharIOT product requires:
	-A JavaScript-enabled Web browser (may be unreliable on IE)
	-Python 3.x
	-Any operating system

1. In a command line, change the directory to the location of geolinks.html.
2. Run the following command:
	python3 -m http.server
3. Use your web browser to visit localhost:8000/geolinks.html

MAP VIEW
1. Click on your favorite state.
2. Click on your favorite region of that state.
3. Click to select your favorite ZIP codes. You can also drag a box to select many quickly.
4. Double click to zoom back out to the whole U.S.
5. Click "Show me the money!" You're now looking at every ZIP code your ZIP codes' nonprofits made grants to.
6. Use the filters at the top to drill down to years, categories, and flow directions you're interested in. You can also use the scroll wheel/two fingers to zoom in and out on the map, and you can drag it around.
7. You can switch to the "donor habits" view, where you can see places that donors typically donated to alongside your geographic area.

GRAPH VIEW
1. Once you're bored with Map View, use the radio button to switch to Graph View.
2. Similar to Map View, you can zoom and pan the view, and make categorical selections using the buttons and menus at the top.
3. Coloration of the nodes can be changed from socioeconomic clusters to net grant flows at the click of a button on the left menu.