from .config_paths import ConfigPaths


def setup_runtime(paths: ConfigPaths, safe: bool = True, setup_logs: bool = True):
    if safe:
        for d in paths.get_all_directories():
            d.mkdir(parents=True, exist_ok=True)

    if setup_logs:
        from ..logs.log_manager import LogManager
        from ..logs.log_setup import setup_logging
        from ..config.path_structs import LogPaths
        
        log_paths = LogPaths(
            csv_log_dir=paths.csv_log_dir,
            python_log_dir=paths.python_log_dir,
            split_log_dir=paths.split_log_dir
        )
        log_manager = LogManager(log_paths)
        python_log_path = log_manager.generate_python_log()
        setup_logging(python_log_path)