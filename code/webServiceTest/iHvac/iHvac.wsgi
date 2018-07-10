import sys

activate_this='C:/Users/controlslab/webEnv/Scripts/activate_this.py'

with open(activate_this) as file_:
	exec(file_.read(), dict(__file__=activate_this))

sys.path.insert(0,'C:\Apache24\wsgi')

import iHvac
#from iHvac import app as application
application = iHvac.create_app()