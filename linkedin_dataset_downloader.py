import kagglehub # pip install kagglehub
import shutil
import os

dataset_collection = {
    # "upwork_2024":  "asaniczka/upwork-job-postings-dataset-2024-50k-records",
    # "upwork_before_2024": "ahmedmyalo/upwork-freelance-jobs-60k",
    # "indeed": "promptcloud/indeed-job-posting-dataset",
    "linkedin_23": "arshkon/linkedin-job-postings",
    # "linkedin_24": "asaniczka/1-3m-linkedin-jobs-and-skills-2024"
}

for dataset, source in dataset_collection.items ():
    # download dataset
    path = kagglehub.dataset_download(source)
    # create a destination for the dataset, all of them will be stored in the data folder
    destination = os.path.join(os.getcwd(), "data", dataset)
    os.makedirs(destination, exist_ok=True)

    # copy from the .cache due to dataset's download default storage in my local is <user_name>/.cache folder
    shutil.copytree(path, destination, dirs_exist_ok=True)

    print("Path to dataset files:", path)
    print("destination data files: ", destination);

    # remove the .cache
    if os.path.exists(path):
        shutil.rmtree(os.path.expanduser("~/.cache/kagglehub"))
        print("Cache deleted:", path)