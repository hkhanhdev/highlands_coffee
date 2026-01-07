import importlib
def load_compute_function(stage: str, table: str):
    """
    Dynamically load compute function based on stage + table.
    Expects module structure: compute/<stage_folder>/<table>.py with function run(...)
    """
    stage_map = {
        "source_2_raw": "s_2_r",
        "raw_2_enriched": "r_2_e",
        "enriched_2_curated": "e_2_c",
    }

    if stage not in stage_map:
        raise ValueError(f"Unknown stage: {stage}")

    stage_folder = stage_map[stage]
    module_path = f"mappings.{table}"

    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError:
        raise Exception(f"Compute module not found: {module_path}")

    if not hasattr(module, stage_folder):
        raise Exception(
            f"Compute module {module_path} missing '{stage_folder}' function"
        )

    compute_fn = getattr(module, stage_folder)

    if not callable(compute_fn):
        raise Exception(f"{module_path}.{stage_folder} is not callable")

    return compute_fn




