import argparse
import os
import sys
import traceback

from etl.transform import Transform
from utl.utls import rtn_logger, spark


def main():
    logger = rtn_logger("Brazilian_E_Commerce")
    logger.info("application start running...")
    parser = argparse.ArgumentParser(description="Brazilian_E_Commerce")
    parser.add_argument("-e", "--envt",
                        help="please confirm the environment",
                        nargs='?',
                        default="DEV"
                        )
    args = parser.parse_args()
    env = args.envt
    logger.info(f"application run environment is {env}")
    config_dir = os.path.join(os.path.dirname(__file__), "..", "config")
    settings_file = os.path.join(config_dir, "settings.yaml")

    try:
        t = Transform(settings_file, env)
        t.pipeline()
    except Exception as e:
        logger.error(f"exception is captured : \n {e}")
        logger.error(f"trace back is  : \n {traceback.format_exc()}")
        sys.exit(1)
    else:
        logger.info("Brazilian_E_Commerce run completed!")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
