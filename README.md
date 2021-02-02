# SatCube
SatCube is an end to end satellite processing software, developed to create Analysis Ready Data. It's main goal 
is to download and preprocess Sentinel and Landsat images in order to serve them as a corrected, unified product.

## Installation
Use pip to install requirements. Optionally create a python virtual environment.

```bash
python -m venv satcube_env 
pip install -r requirements.txt
```

## Usage
Simple examples given bellow to expose the main functionality.
```python
import satcube

datacube = satcube.download(lat="30", lon="20", "27-2-2020") # returns images on given date
datacube_corrected = datacube.process() # makes default corrections on image
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[GNU GPLv3](https://choosealicense.com/licenses/gpl-3.0/)
