from typing import Callable, Optional
from logging_config import LoggingConfig

# Create logger
logger = LoggingConfig('progress_manager').setup_logger()

# Type alias for progress callback
ProgressCallback = Callable[[str, int], None]

class ProgressManager:
    def __init__(self, is_sub_workflow: bool = False):
        """
        Initialize progress manager.
        
        Args:
            is_sub_workflow (bool): If True, this is part of a larger workflow and should use 0-50% range.
                                  If False, use full 0-100% range.
        """
        self.is_sub_workflow = is_sub_workflow
        logger.info(f"Initialized ProgressManager (sub_workflow: {is_sub_workflow})")

    def get_progress(self, base_progress: int) -> int:
        """
        Calculate actual progress based on whether this is a sub-workflow.
        
        Args:
            base_progress (int): Base progress value (0-100)
        
        Returns:
            int: Adjusted progress value
        """
        if self.is_sub_workflow:
            # When part of a larger workflow, use 0-50% range
            return base_progress // 2
        # When running independently, use full 0-100% range
        return base_progress

    def update_progress(self, 
                       message: str, 
                       base_progress: int, 
                       callback: Optional[ProgressCallback] = None) -> None:
        """
        Update progress with message if callback is provided.
        
        Args:
            message (str): Progress message to display
            base_progress (int): Base progress value (0-100)
            callback (Optional[ProgressCallback]): Progress callback function
        """
        if callback:
            adjusted_progress = self.get_progress(base_progress)
            callback(message, adjusted_progress)
            logger.info(f"Progress update: {message} ({adjusted_progress}%)")

class SQLProgressStages:
    """Progress stages for SQL workflow"""
    GENERATE_SQL = 20
    VERIFY_INTENT = 40
    VALIDATE_SQL = 60
    EXECUTE_QUERY = 80
    FORMAT_RESPONSE = 100

class AnalyticsProgressStages:
    """Progress stages for Analytics workflow"""
    PARSE_DATA = 60
    STATISTICAL_ANALYSIS = 70
    TRENDS_ANALYSIS = 80
    GENERATE_INSIGHTS = 90
    CREATE_VISUALIZATIONS = 95
    FORMAT_RESPONSE = 100 