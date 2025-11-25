import pathlib

__PACKAGE_ROOT__ = pathlib.Path(__file__).parent

def _construct_filepaths(filepath: str, package_root: pathlib.Path = __PACKAGE_ROOT__) -> pathlib.Path:
    return package_root / filepath

def resolve_filename(filename: str) -> str:
    return pathlib.Path(filename).stem.lower()

def get_test_configs(filename: str) -> dict:
    test_configs = TEST_FILE_PATHS.get(resolve_filename(filename))
    if test_configs is None:
        raise ValueError(f"Test config not found for {resolve_filename(filename)}")
    return test_configs

TEST_FILE_PATHS = {
    "test_macro_default_channel_grouping": {
        "source_category_mapping": _construct_filepaths("seeds/ga4_source_categories.csv"),
        "macro_to_test": _construct_filepaths("macros/default_channel_grouping.sql"),
    },
    "test_macro_exclude_query_parameters": {
        "macro_to_test": _construct_filepaths("macros/url_parsing.sql"),
    },
    "test_macro_extract_query_parameter_value": {
        "macro_to_test": _construct_filepaths("macros/url_parsing.sql"),
    },
    "test_stg_ga4__derived_session_properties": {
        "actual": _construct_filepaths("models/staging/stg_ga4__derived_session_properties.sql"),
        "unnest_key": _construct_filepaths("macros/unnest_key.sql"),
    },
    "test_stg_ga4__derived_user_properties": {
        "actual": _construct_filepaths("models/staging/stg_ga4__derived_user_properties.sql"),
        "unnest_key": _construct_filepaths("macros/unnest_key.sql"),
    },
    "test_stg_ga4__event_to_query_string_params": {
        "actual": _construct_filepaths("models/staging/stg_ga4__event_to_query_string_params.sql"),
    },
    "test_stg_ga4__page_conversions": {
        "actual": _construct_filepaths("models/staging/stg_ga4__page_conversions.sql"),
        "valid_column_name": _construct_filepaths("macros/valid_column_name.sql"),
    },
    "test_stg_ga4__session_conversions_daily": {
        "actual": _construct_filepaths("models/staging/stg_ga4__session_conversions_daily.sql"),
        "valid_column_name": _construct_filepaths("macros/valid_column_name.sql"),
    },
    "test_stg_ga4__sessions_traffic_sources_last_non_direct_daily": {
        "actual": _construct_filepaths("models/staging/stg_ga4__sessions_traffic_sources_last_non_direct_daily.sql"),
    },
    "test_stg_ga4__user_id_mapping": {
        "actual": _construct_filepaths("models/staging/stg_ga4__user_id_mapping.sql"),
    },
    "test_stg_ga4__users_first_last_events": {
        "actual": _construct_filepaths("models/staging/stg_ga4__client_key_first_last_events.sql"),
    },
}
