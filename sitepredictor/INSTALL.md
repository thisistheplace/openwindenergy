# Site Predictor - Installation

## 1. Install Open Wind Energy on local machine

The Open Wind Energy Site Predictor requires a local installation of the main Open Wind Energy application. Refer to the [Open Wind Energy Installation Guide](../INSTALL.md) for how to install Open Wind Energy locally. 

## 2. Run Open Wind Energy build

Once Open Wind Energy is installed locally, run a full build by typing:

```
cd /path/to/openwindenergy/
./build-cli.sh
```


## 2. Enable virtual environment

Ensure the virtual Python environment creating during `1`, above, is activated:

```
cd /path/to/openwindenergy/
source venv/bin/activate
```

## 3. Run Site Predictor

```
cd sitepredictor/
python sitepredictor.py
```

Using the default resolution of `1000km`, a full run of Site Predictor takes 5-10 hours and will create a final probability GeoTIFF raster at `sitepredictor/output/output-machinelearning.tif`. 

You can modify the default resolution by... [to come]