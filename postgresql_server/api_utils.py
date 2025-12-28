from models.base import MyBase
from typing import Union, List
import math

def _convert_nan_to_none(obj: Union[dict, list, float]):
    """
    Converts NaN and Infinity float values to None in a nested structure of dictionaries, lists, or other objects.

    This function recursively checks if any value in the given object (which can be a dictionary, list, or other object)
    is a float that is NaN (Not a Number) or Infinity (positive or negative). If such values are found, they are converted
    to None. This is helpful for serializing responses where NaN or Infinity are not JSON-compliant.

    :param obj: The object to convert, which can be a dictionary, list, or any other object.
    :type obj: Union[dict, list, float, Any]

    :return: The same object with NaN and Infinity values converted to None.
    :rtype: Union[dict, list, float, Any]

    :raises TypeError: If the provided object is of an unsupported type.

    **Example usage**:

    .. code-block:: python

        obj = {"value": float('nan'), "nested": [1, 2, float('inf')]}
        result = _convert_nan_to_none(obj)
        print(result)
        # Output: {'value': None, 'nested': [1, 2, None]}
    """
    if isinstance(obj, dict):
        return {key: _convert_nan_to_none(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_convert_nan_to_none(item) for item in obj]
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None  # Convert NaN or infinity to None
    return obj

def response_json_wrapper(orm_result: Union[MyBase, List[MyBase]]):
    """
    Converts an ORM result to a JSON-compatible structure.

    This function takes an ORM result (which could either be a single instance of `MyBase` 
    or a list of `MyBase` instances), converts it to a dictionary using the `to_dict()` 
    method of `MyBase`, and recursively replaces any NaN values in the result with None 
    using `_convert_nan_to_none()`.

    Args:
        orm_result (Union[MyBase, List[MyBase]]): The ORM result to be converted. It can either
                                                  be a single `MyBase` instance or a list of them.

    Returns:
        Union[dict, List[dict]]: A JSON-compatible dictionary or list of dictionaries, 
                                  where all NaN values have been replaced with None.
    """
    if isinstance(orm_result, list):
        return [_convert_nan_to_none(ent.to_dict()) for ent in orm_result]
    return _convert_nan_to_none(orm_result.to_dict())
