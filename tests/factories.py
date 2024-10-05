from faker import Faker
import json
import os
import pandas as pd
import random


def build_data_asset(**kwargs):
    faker = Faker()
    timestamp = faker.iso8601()
    planet = random.choice(["mercury", "venus", "earth", "mars", "jupiter", "saturn", "uranus", "neptune", "pluto"])
    readings = {f"value{i}": random.randint(0, 100) for i in range(1, random.randint(0, 100))}

    return strict_merge(
        {
            "timestamp": timestamp,
            "dataAsset": planet,
            "iotreadings": readings,
        },
        kwargs,
    )


def strict_merge(base_dict, kwargs):
    allowed_keys = base_dict.keys()
    if not set(kwargs.keys()).issubset(allowed_keys):
        invalid_keys = set(kwargs.keys()) - allowed_keys
        raise ValueError(f"Invalid keys in kwargs: {invalid_keys}")

    return {
        **base_dict,
        **kwargs,
    }


def dump_raw_data_file(data_assets, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        json.dump(data_assets, f)


def build_parquet_dataframe(**kwargs):
    faker = Faker()
    timestamp = faker.iso8601()
    planet = random.choice(["mercury", "venus", "earth", "mars", "jupiter", "saturn", "uranus", "neptune", "pluto"])
    iotreadings_count = kwargs.pop("iotreadings_count", 0)
    readings = {
        f"iotreadings_value{i}": random.randint(0, 100) for i in range(1, random.randint(iotreadings_count, 100))
    }
    row = strict_merge(
        {
            "timestamp": timestamp,
            "dataAsset": planet,
            **readings,
        },
        kwargs,
    )

    return pd.DataFrame([row])


def dump_parquet_file(df, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_parquet(file_path)
