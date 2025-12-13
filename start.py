import logging

from awsl_pic_pipeline.migration import migration

logging.basicConfig(
    format="%(asctime)s: %(levelname)s: %(name)s: %(message)s",
    level=logging.INFO
)


migration()
