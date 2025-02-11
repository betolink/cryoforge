import unittest
from pathlib import Path
import tempfile
import pystac

# Assuming the function to generate STAC item is in a module named `stac_generator`
from cryoforge import generate_itslive_metadata, save_metadata

class TestSTACItemGeneration(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory to store the output STAC item JSON
        self.temp_dir = tempfile.TemporaryDirectory()

        # Path to a sample NetCDF file (replace with your actual path)
        self.sample_netcdf_paths = [
            "https://its-live-data.s3.amazonaws.com/velocity_image_pair/landsatOLI/v02/N60W050/LT05_L1TP_009011_19920706_20200914_02_T1_X_LT05_L1TP_009011_19930506_20200914_02_T1_G0120V02_P011.nc",
            "https://its-live-data.s3.amazonaws.com/velocity_image_pair/sentinel2/v02/N60W010/S2A_MSIL1C_20170711T125301_N0205_R138_T27VXL_20170711T125302_X_S2A_MSIL1C_20170830T125301_N0205_R138_T27VXL_20170830T125302_G0120V02_P080.nc",
            "https://its-live-data.s3.amazonaws.com/velocity_image_pair/landsatOLI/v02/N60W050/LE07_L1TP_009011_20090424_20200912_02_T1_X_LE07_L1TP_009011_20090915_20200911_02_T1_G0120V02_P011.nc",
            "s3://its-live-data/velocity_image_pair/landsatOLI/v02/N60W010/LC08_L1TP_219014_20140328_20200911_02_T1_X_LC08_L1TP_219014_20140413_20200911_02_T1_G0120V02_P085.nc"                
        ]

    def tearDown(self):
        # Clean up the temporary directory
        self.temp_dir.cleanup()

    def test_generate_metadata(self):
        output_path = Path(self.temp_dir.name)

        for url in self.sample_netcdf_paths:
            metadata = generate_itslive_metadata(url)

            self.assertTrue(isinstance(metadata["stac"], pystac.Item))
            self.assertTrue(metadata["stac"].validate())

            tmp_path = output_path / Path(metadata["stac"].id.replace(".nc", ""))
            tmp_path.mkdir(exist_ok=True)

            save_metadata(metadata, str(tmp_path))
            self.assertTrue(tmp_path.exists())
            metadata_extensions = {".json", ".premet", ".spatial"}
    
            existing_extensions = {file.suffix for file in tmp_path.iterdir() if file.is_file()}
    
            self.assertTrue(metadata_extensions.issubset(existing_extensions), f"Missing files with extensions: {metadata_extensions - existing_extensions}")



if __name__ == "__main__":
    unittest.main()
