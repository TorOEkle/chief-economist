import functools
import time
from typing import Any, Callable

from dagster import (
    DynamicPartitionsDefinition,
    MonthlyPartitionsDefinition,
    StaticPartitionsDefinition,
)


# Define numeric partitions
adress_partitions = StaticPartitionsDefinition(['0', '50000'])

# Månadlig partitionering
# Den starter den 1. månaden efter start_date, end_offset = 1 ==> upp til dagens datum.
monthly_partition = MonthlyPartitionsDefinition(start_date="2014-12-24", end_offset=1)

# Partitionering för Excel-filer (Avtal och uppföljning)
filename_partition = DynamicPartitionsDefinition(name="filenames")

class Logg:
    """
    Logg is a helper class designed to log messages within a given context.
    It contains static methods that can be used to log informational and error messages.
    
    Static methods:
        info(context, msg: str) -> None:
            Logs an informational message using the context's logging mechanism,
            if a context is available.
        error(context, msg: str) -> None:
            Logs an error message using the context's logging mechanism,
            if a context is available.
    """

    @staticmethod
    def info(context, msg):
        if context:
            context.log.info(msg)

    @staticmethod
    def error(context, msg):
        if context:
            context.log.error(msg)

    @staticmethod
    def debug(context, msg):
        if context:
            context.log.debug(msg)

    @staticmethod
    def warning(context, msg):
        if context:
            context.log.warning(msg)

    @staticmethod
    def critical(context, msg):
        if context:
            context.log.critical(msg)

#########################
## D E C O R A T O R S ##
#########################
# Igjen takk til kollega Jens Klarèn

"""Fördelar med Decorators
- Mindre upprepning av kod - Ingen manuell start_time = time.time() överallt
- Konsistent loggning - Alla assets får samma format på tidsmätning
- Automatisk metadata - Execution time läggs automatiskt till i Dagster metadata
- Lättare att underhålla - Centraliserad logik för cross-cutting concerns
- Validering - Automatisk kontroll av parametrar vid runtime
"""

def measure_time_with_metadata(func: Callable) -> Callable:
    """
    Decorator för att mäta exekveringstid och returnera den som metadata.

    Specifikt designad för Dagster assets som returnerar MaterializeResult.
    Lägger till execution_time_seconds i metadata.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()

        result = func(*args, **kwargs)

        end_time = time.time()
        execution_time = end_time - start_time

        # Om resultatet har metadata, lägg till execution_time
        if hasattr(result, "metadata") and result.metadata is not None:
            # Skapa ny metadata dictionary med execution_time
            from dagster import MetadataValue

            new_metadata = dict(result.metadata)
            new_metadata["execution_time_seconds"] = MetadataValue.float(execution_time)
            new_metadata["execution_time_formatted"] = MetadataValue.text(
                f"{int(execution_time // 60)}m {execution_time % 60:.2f}s"
            )

            # Skapa nytt MaterializeResult med uppdaterad metadata
            from dagster import MaterializeResult

            result = MaterializeResult(
                metadata=new_metadata,
            )

        return result

    return wrapper


def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff_multiplier: float = 2.0):
    """
    Decorator för att återförsöka operationer vid fel.

    Args:
        max_retries: Maximalt antal återförsök
        delay: Initial fördröjning mellan försök (sekunder)
        backoff_multiplier: Multiplikator för exponential backoff
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            # Försök hitta context för loggning
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs:
                context = kwargs["context"]

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    if attempt < max_retries:
                        wait_time = delay * (backoff_multiplier**attempt)
                        error_msg = (
                            f"Försök {attempt + 1}/{max_retries + 1} misslyckades för {func.__name__}: {e}. "
                            f"Väntar {wait_time:.1f}s innan nästa försök."
                        )

                        if context:
                            Logg.warning(context, error_msg)
                        else:
                            print(f"WARNING: {error_msg}")

                        time.sleep(wait_time)
                        continue
                    break

            # Alla försök misslyckades
            error_msg = f"Alla {max_retries + 1} försök misslyckades för {func.__name__}"
            if context:
                Logg.error(context, error_msg)
            else:
                print(f"ERROR: {error_msg}")

            raise last_exception

        return wrapper

    return decorator

