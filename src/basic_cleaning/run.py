#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import os
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type='basic_cleaning')
    run.config.update(args)

    artifact = run.use_artifact(args.input_artifact)
    artifact_path = artifact.file()

    df = pd.read_csv(artifact_path)

    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    df['last_review'] = pd.to_datetime(df['last_review'])

    filename = 'clean_sample.csv'
    df.to_csv(filename, index=False)

    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(filename)
    run.log_artifact(artifact)

    os.remove(filename)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='This step cleans the data')


    parser.add_argument(
        '--input_artifact',
        type=str,
        help='Name of the input artifact',
        required=True
    )

    parser.add_argument(
        '--output_artifact',
        type=str,
        help='Name of the output artifact',
        required=True
    )

    parser.add_argument(
        '--output_type',
        type=str,
        help='Type of the output artifact',
        required=True
    )

    parser.add_argument(
        '--output_description',
        type=str,
        help='Description of the output artifact',
        required=True
    )

    parser.add_argument(
        '--min_price',
        type=float,
        help='Minimum price for filtering the dataset',
        required=True
    )

    parser.add_argument(
        '--max_price',
        type=float,
        help='Maximum price for filtering the dataset',
        required=True
    )


    args = parser.parse_args()

    go(args)
