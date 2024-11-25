import typing as t

import asyncio
import dlt
import time
import random


def _sleep(seconds: int):
    time.sleep(seconds)


@dlt.resource(
    parallelized=True,
)
def _sleep_resource():
    for idx in range(100):
        seconds = random.randint(1, 5)
        yield idx, seconds


@dlt.transformer(
    parallelized=True,
)
async def _sleep_transformer(record):
    idx, seconds = record
    print(f"[{idx}] Sleeping for {seconds} seconds")
    _sleep(seconds)
    return idx, seconds


def load():
    dlt.secrets["destination.filesystem.bucket_url"] = "output"
    dlt.config["extract.workers"] = 4

    pipeline = dlt.pipeline(
        pipeline_name="parallel_example_pipeline",
        dataset_name="parallel_dataset",
        progress=dlt.progress.tqdm(colour="yellow"),
        destination="filesystem",
    )

    info = pipeline.run(
        _sleep_resource() | _sleep_transformer,
    )


if __name__ == "__main__":
    load()
