#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_sprout_social import SourceSproutSocial

if __name__ == "__main__":
    source = SourceSproutSocial()
    launch(source, sys.argv[1:])
