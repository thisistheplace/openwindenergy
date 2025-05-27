from collections.abc import Mapping
import multiprocessing as mp
import subprocess
from typing import Any, Optional

from ..constants import *

LOG = mp.get_logger()


def run_subprocess(
    args,
    env: Optional[Mapping[str, Any]],
    return_bool: bool = False,
    log_output: bool = False,
) -> str:
    """
    Runs subprocess with environment variables
    """

    global SERVER_BUILD, USE_MULTIPROCESSING

    if (not SERVER_BUILD) and (not USE_MULTIPROCESSING):
        if args[0] == "ogr2ogr":
            args.append("-progress")

    output = subprocess.run(args, env=env, capture_output=log_output, text=log_output)

    if output.returncode != 0:
        msg = f"subprocess.run failed with error code: {output.returncode}\n{' '.join(args)}"
        LOG.error(msg)
        raise RuntimeError(msg)

    if return_bool:
        return True
    return " ".join(args)
