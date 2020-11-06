import os
import time
from datetime import datetime

while True:
    print(f'Time to Refresh PAK IRSA sheet! - {str(datetime.now())}')
    os.system('python main.py')
    time.sleep(60*60*2)
