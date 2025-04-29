import pandas as pd
from datetime import datetime
import threading

class DBLogger:
    def __init__(self):
        self.logs = []
        self.lock = threading.Lock()

    def log(self, function, action, params, result):
        with self.lock:
            self.logs.append({
                "timestamp": datetime.now().isoformat(),
                "function": function,
                "action": action,
                "params": str(params),
                "result": str(result)
            })

    def save_to_excel(self, path):
        with self.lock:
            if not self.logs:
                return
            df = pd.DataFrame(self.logs)
            df.to_excel(path, index=False)

# Create a global logger instance

db_logger = DBLogger() 