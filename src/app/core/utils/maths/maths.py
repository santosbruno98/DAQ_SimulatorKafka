''' Mathematical functions for Correlation and Conversion. '''
# -------------------------
# Utility / Domain methods
# -------------------------

import pickle
import zlib

import numpy as np


def points_fibers(
    min_temperature: int,
    max_temperature: int,
    min_humidity: int,
    max_humidity: int,
    x_average: int,
    check_reversed: bool,
    points_sensor: dict[int, tuple[int, int]],
) -> tuple[list[int], list[int], list[int], list[int]]:
    """
    Compute grid points for temperature / humidity sampling and map sensors to points.

    Returns:
        (points_temperature, points_humidity, temperature_sensor_per_point, humidity_sensor_per_point)
    """
    step = 2 * x_average if x_average != 0 else 1

    if check_reversed and min_humidity != min_temperature:
        if min_humidity < min_temperature:
            points_humidity = list(
                range(min_humidity + x_average, max_humidity - x_average + 1, step)
            )
            points_temperature = list(
                reversed(
                    range(
                        min_temperature + (max_humidity - points_humidity[-1]),
                        max_temperature - x_average + 1,
                        step,
                    )
                )
            )
            humidity_sensor_per_point = list(
                map(
                    lambda point_humidity: list(
                        filter(
                            lambda sensor_index: points_sensor[sensor_index][0]
                            <= point_humidity
                            <= points_sensor[sensor_index][1],
                            list(points_sensor.keys()),
                        )
                    )[0],
                    points_humidity,
                )
            )
            temperature_sensor_per_point = humidity_sensor_per_point
        else:  # min_temperature < min_humidity
            points_temperature = list(
                range(
                    min_temperature + x_average, max_temperature - x_average + 1, step
                )
            )
            points_humidity = list(
                reversed(
                    range(
                        min_humidity + (max_temperature - points_temperature[-1]),
                        max_humidity - x_average + 1,
                        step,
                    )
                )
            )
            temperature_sensor_per_point = list(
                map(
                    lambda point_temperature: list(
                        filter(
                            lambda sensor_index: points_sensor[sensor_index][0]
                            <= point_temperature
                            <= points_sensor[sensor_index][1],
                            list(points_sensor.keys()),
                        )
                    )[0],
                    points_temperature,
                )
            )
            humidity_sensor_per_point = temperature_sensor_per_point
    else:
        points_temperature = list(
            range(min_temperature + x_average, max_temperature - x_average + 1, step)
        )
        points_humidity = list(
            range(
                min_humidity + (max_temperature - points_temperature[-1]),
                max_humidity - x_average + 1,
                step,
            )
        )
        temperature_sensor_per_point = list(
            map(
                lambda point_temperature: list(
                    filter(
                        lambda sensor_index: points_sensor[sensor_index][0]
                        <= point_temperature
                        <= points_sensor[sensor_index][1],
                        list(points_sensor.keys()),
                    )
                )[0],
                points_temperature,
            )
        )
        humidity_sensor_per_point = list(
            map(
                lambda point_humidity: list(
                    filter(
                        lambda sensor_index: points_sensor[sensor_index][0]
                        <= point_humidity
                        <= points_sensor[sensor_index][1],
                        list(points_sensor.keys()),
                    )
                )[0],
                points_humidity,
            )
        )

    return (
        points_temperature,
        points_humidity,
        temperature_sensor_per_point,
        humidity_sensor_per_point,
    )


def temperature_humidity_calculation(
    data: np.ndarray,
    electrical_data: np.ndarray,
    distance_average: int,
    slope_temperature: float,
    slope_humidity: float,
    slope_temperature_fiber_rh: float,
    points_temperature: list[int],
    points_humidity: list[int],
    temperature_sensor_per_point: list[int],
    humidity_sensor_per_point: list[int],
) -> np.ndarray:
    """
    Temperature and humidity calculation based on correlation `data` and `electrical_data`.

    Args:
        data: correlation data shape (n_sweeps?, n_timestamps?, n_distance_points?) or similar
        electrical_data: electrical array with shape (2, n_distance_points) (two channels)
        distance_average: window half-width for averaging
        slope_temperature: calibration slope for temperature
        slope_humidity: calibration slope for humidity
        slope_temperature_fiber_rh: cross-term slope
        points_temperature/humidity: indices to evaluate
        temperature_sensor_per_point/humidity_sensor_per_point: mapping

    Returns:
        fibers_data: np.ndarray shape (2, electrical_channels, n_points)
    """
    # compute frequency shifts
    if distance_average != 0:
        frequency_shift_temperature = np.transpose(
            np.array(
                list(
                    map(
                        lambda point_temperature: np.average(
                            data[
                                0,
                                :,
                                point_temperature
                                - distance_average : point_temperature
                                + distance_average
                                + 1,
                            ],
                            axis=1,
                        ),
                        points_temperature,
                    )
                )
            )
        )
        frequency_shift_humidity = np.transpose(
            np.array(
                list(
                    map(
                        lambda point_humidity: np.average(
                            data[
                                0,
                                :,
                                point_humidity
                                - distance_average : point_humidity
                                + distance_average
                                + 1,
                            ],
                            axis=1,
                        ),
                        points_humidity,
                    )
                )
            )
        )
    else:
        frequency_shift_temperature = np.transpose(
            np.array(
                list(
                    map(
                        lambda point_temperature: data[0, :, point_temperature],
                        points_temperature,
                    )
                )
            )
        )
        frequency_shift_humidity = np.transpose(
            np.array(
                list(
                    map(
                        lambda point_humidity: data[0, :, point_humidity],
                        points_humidity,
                    )
                )
            )
        )
    temperature_fiber = np.transpose(
        np.array(
            list(
                map(
                    lambda point: (
                        frequency_shift_temperature[:, point] / slope_temperature
                    )
                    + electrical_data[0, int(temperature_sensor_per_point[point])],
                    range(frequency_shift_temperature.shape[1]),
                )
            )
        )
    )

    humidity_fiber = np.transpose(
        np.array(
            list(
                map(
                    lambda point: (
                        (
                            frequency_shift_humidity[:, point]
                            - (
                                (slope_temperature_fiber_rh / slope_temperature)
                                * frequency_shift_temperature[:, point]
                            )
                        )
                        / slope_humidity
                    )
                    + electrical_data[1, int(humidity_sensor_per_point[point])],
                    range(frequency_shift_humidity.shape[1]),
                )
            )
        )
    )
    fibers_data = np.empty(
        (2, humidity_fiber.shape[0], humidity_fiber.shape[1]), dtype=np.float32
    )
    fibers_data[0, :, :] = temperature_fiber
    fibers_data[1, :, :] = humidity_fiber
    return fibers_data


def deserialize_array(data: bytes) -> np.ndarray:
    """
    Decompress and unpickle a bytes message into a NumPy array.
    """
    decompressed: bytes = zlib.decompress(data)
    arr: np.ndarray = pickle.loads(decompressed)
    return arr

