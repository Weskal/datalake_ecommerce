# main.py
import os
from pipeline import run_pipeline

if __name__ == "__main__":

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # pasta onde está o main.py
    data_folder = os.path.join(BASE_DIR, "..", "..", "data")
    data_folder = os.path.abspath(data_folder)

    print("BASE_DIR:", BASE_DIR)
    print("data_folder:", data_folder)


    run_pipeline(data_folder=data_folder)
