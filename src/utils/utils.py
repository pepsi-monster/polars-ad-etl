import polars as pl


def dataframe_to_a1_address(
    df: pl.DataFrame,
    vertical_offset: int | None = None,
    horizontal_offset: int | None = None,
):
    v_offset = vertical_offset or 0
    h_offset = horizontal_offset or 0

    df_shape = df.shape

    df_length = df_shape[0] + 1  # Including header row
    df_width = df_shape[1]

    def _int_to_bijective_base_26(n: int) -> str:
        s = ""
        while n > 0:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    a1_start = _int_to_bijective_base_26(1 + h_offset)
    int_start = 1 + v_offset
    a1_end = _int_to_bijective_base_26(df_width + h_offset)
    int_end = df_length + v_offset

    a1_address = f"{a1_start}{int_start}:{a1_end}{int_end}"

    return a1_address
