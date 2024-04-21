import time
import os
import logging
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pymongo import MongoClient
import casparser
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
mongo_uri = os.getenv('MONGO_URI')
db_name = os.getenv('DB_NAME')
files_collection_name = 'file_credentials'
parsed_data_collection_name = 'parsed_cas_data'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MongoDB setup
client = MongoClient(mongo_uri)
db = client[db_name]
files_collection = db[files_collection_name]
parsed_data_collection = db[parsed_data_collection_name]

# Watcher setup
class Watcher:
    DIRECTORY_TO_WATCH = os.getenv('DIRECTORY_TO_WATCH')

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=False)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except KeyboardInterrupt:
            self.observer.stop()
            logger.info("Observer Stopped")
        self.observer.join()

class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.pdf'):
            with ThreadPoolExecutor(max_workers=10) as executor:
                executor.submit(process_file, event.src_path)

def process_file(file_path):
    logger.info(f"Processing file: {file_path}")
    file_name = os.path.basename(file_path)
    file_data = files_collection.find_one({'file_name': file_name})
    if file_data:
        password = file_data['password']
        parsed_data = casparser.read_cas_pdf(file_path, password, output="json")
        parsed_data_collection.insert_one({'file_name': file_name, 'data': json.loads(parsed_data)})
        logger.info(f"File {file_name} parsed and data stored.")
        try:
            os.remove(file_path)
            logger.info(f"File {file_name} has been deleted after processing.")
        except OSError as e:
            logger.error(f"Error: {e.strerror} - {e.filename}.")

if __name__ == "__main__":
    w = Watcher()
    w.run()
