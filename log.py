import logging
import inspect


def init(level=logging.DEBUG):
    """ Initialize logging """
    logging.basicConfig(
        level=level,
        # format='%(asctime)s - %(message)s',
        filename='logging.ini'
    )


def get_outer_logger():
    return logging.getLogger(
        inspect.currentframe().f_back.f_back.f_globals["__name__"]
    )


def info(msg, *args, **kwargs):
    return get_outer_logger().info(msg, *args, **kwargs)

