def format_float(value: float):
    """
    Formats float value to be short and readable
    """

    return str(round(value, 1)).replace(".0", "")
