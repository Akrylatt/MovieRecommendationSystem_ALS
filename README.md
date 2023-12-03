# Movie Recommender System

This repository contains the solution for Assignment 2 of the PMDL course, implementing a movie recommendation system using Alternating Least Squares (ALS). The system is designed to provide personalized movie recommendations based on user preferences.

**Author:** Adela Krylova
**Email:** a.krylova@innopolis.university
**Group Number:** BS20-AI-01

## Repository Structure

- `data`: This directory is organized into subdirectories for different types of data (All data are provided as a .txt file with a link for downloading, since to upload all the files would be too heavy for github)
  - `external`: Data from third-party sources.
  - `interim`: Intermediate data that has been transformed during the process.
  - `raw`: The original, immutable data.

- `models`: Trained and serialized models, as well as final checkpoints.

- `notebooks`: Jupyter notebook with the solution

- `references`: Data dictionaries, manuals, and other explanatory materials.

- `reports`: Contains the final report in PDF format.
  - `final_report.pdf`: Comprehensive report covering data exploration, solution exploration, training process, and evaluation.

- `benchmark`: Includes data and scripts for evaluating the model.
  - `data`: Dataset used for evaluation.
  - `evaluate.py`: Script that performs the evaluation of the given model.

## Usage
1. Open the Collab notebook
2. Start Pyspark session
3. Train the model or load the pre-trained one
4. Test using your data (put them into evaluation folder as .csv file)

## Evaluation

Explain how to use the provided benchmarking script for evaluating the model's performance.

```bash
# Example command to evaluate the model
python benchmark/evaluate.py
```
Make sure that you have a folder data with a file with your own users for which the prediction should be made

## Acknowledgments

This project was built based on a 9th lab Assignment provided on Big Data course at ITS, Surabaya, Indonesia
