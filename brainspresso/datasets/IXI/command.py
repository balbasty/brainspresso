from cyclopts import App
from brainspresso.cli import app

ixi_help = """
IXI dataset (controls)

* **Project**       Information eXtraction from Images (IXI)
* **Modalities**    T1w, T2w, PD2, MRA, DTI
* **Populations**   Controls
* **Funding**       EPSRC GR/S21533/02
* **License**       CC BY-SA 3.0
* **URL**           https://brain-development.org/ixi-dataset/
"""

app.command(ixi := App(name="ixi", help=ixi_help))
