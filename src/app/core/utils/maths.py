"""Mathematical functions for Correlation and Conversion."""
# -------------------------
# Utility / Domain methods
# -------------------------

import pickle
import zlib

import numpy as np
from numba import prange


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
                                point_temperature - distance_average : point_temperature
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
                                point_humidity - distance_average : point_humidity
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


def frequency_axis_laser(
    sweeps_mode,
    sweeps_mode_initial,
    sweeps_mode_final,
    sweeps_mode_step,
    current_frequency_step_a,
    current_frequency_step_b,
    temperature_frequency_step,
):
    sweeps_mode_steps = np.arange(
        sweeps_mode_initial, sweeps_mode_final + sweeps_mode_step, sweeps_mode_step
    )
    if sweeps_mode.lower() == "temperature":
        frequency_axis = (
            np.arange(-len(sweeps_mode_steps) + 1, len(sweeps_mode_steps))
            * temperature_frequency_step
            * sweeps_mode_step
        )

        return frequency_axis
    if sweeps_mode.lower() == "current":
        current_steps = np.arange(
            sweeps_mode_initial, sweeps_mode_final + sweeps_mode_step, sweeps_mode_step
        )
        frequency_axis = (
            current_frequency_step_a * current_steps**2
            + current_frequency_step_b * current_steps
        ) - (
            current_frequency_step_a * sweeps_mode_initial**2
            + current_frequency_step_b * sweeps_mode_initial
        )
        frequency_axis = np.concatenate((-frequency_axis[1:][::-1], frequency_axis))

        return frequency_axis

    else:
        print("FAILED TO CALCULATE FREQUENCY AXIS OF LASER")


# @njit()
def smooth_polynomial_peak_finding(x_axis, correlation, smooth_window):
    id_signal_max = np.argmax(correlation)

    if smooth_window > len(
        x_axis[id_signal_max - smooth_window : id_signal_max + 1]
    ) or smooth_window > len(x_axis[id_signal_max : id_signal_max + smooth_window + 1]):
        window_new = smooth_window
        for i in range(1, window_new + 1):
            i = window_new - i
            if i > len(x_axis[id_signal_max - i : id_signal_max + 1]) or i > len(
                x_axis[id_signal_max : id_signal_max + i + 1]
            ):
                continue
            else:
                window_new = i
                break
    else:
        window_new = smooth_window
    if window_new == 0:
        maximizer = id_signal_max

    else:
        signal_peak = correlation[
            id_signal_max - window_new : id_signal_max + window_new + 1
        ]
        x_axis_peak = x_axis[
            id_signal_max - window_new : id_signal_max + window_new + 1
        ]
        polynomial_coefficient = np.polyfit(x_axis_peak, signal_peak, 2)
        maximizer = -polynomial_coefficient[1] / (2 * polynomial_coefficient[0])

    return maximizer


# @njit()
def max_peak_finding(x_axis, correlation):
    margin = 0
    signal_filter = np.array(
        [0] * margin + [1] * (len(correlation) - 2 * margin) + [1] * margin
    )
    peak_index = np.argmax(correlation * signal_filter)
    maximizer = x_axis[peak_index]

    return maximizer


def zero_mean(data):
    mean = np.mean(data, 0, keepdims=True)
    result = data - mean

    return result


def moving_correlation_with_peak_finding(data, frequency_axis, smooth_window):
    normalized_data = zero_mean(data)
    # release data
    del data
    peaks = moving_numba_acceleration(normalized_data, frequency_axis, smooth_window)

    return peaks


# @njit()
def moving_numba_acceleration(data, frequency_axis, smooth_window):
    currents, times, points = data.shape
    results = np.zeros((1, times - 1, points))
    for time in range(times - 1):
        for point in prange(points):
            correlation = np.correlate(
                data[:, time, point], data[:, time + 1, point], "full"
            )
            peak = smooth_polynomial_peak_finding(
                frequency_axis, correlation, smooth_window
            )
            results[0, time, point] = peak
    return results


# @njit()
def moving_cumulative_calculation(
    frequency_shift_peaks,
    cumulative,
    cumulative_last,
    fiber_t_initial_point,
    fiber_t_final_point,
    fiber_rh_initial_point,
    fiber_rh_final_point,
    threshold,
):
    for time in range(0, frequency_shift_peaks.shape[1]):
        if time == 0:
            cumulative[:, time, :] = frequency_shift_peaks[:, time, :] + cumulative_last
        else:
            cumulative[:, time, :] = (
                frequency_shift_peaks[:, time, :] + cumulative[:, time - 1, :]
            )
        if threshold >= 0:
            stacked_array = np.empty((2, cumulative.shape[2]))
            stacked_array[0] = cumulative[0, time - 1, :]
            stacked_array[1] = cumulative[0, time, :]

            outliers, cumulative[:, time, :] = error_detect_and_replace(
                fiber_t_initial_point,
                fiber_t_final_point,
                fiber_rh_initial_point,
                fiber_rh_final_point,
                stacked_array,
                threshold,
            )

    return cumulative


# @njit
def simple_time_analysis(x_min, x_max, y_values, threshold):
    max_len = x_max - x_min
    indices = np.empty(max_len, dtype=np.int32)
    count = 0
    for x in range(x_min, x_max):
        variation = np.abs(y_values[1, x] - y_values[0, x])
        if variation >= threshold:
            indices[count] = x
            count += 1
    return indices[:count]


# @njit
def substitute_outliers(x_min, x_max, y_values, outlier_indices):
    new_y_values = np.copy(y_values)

    # making the set as a mask
    mask_len = x_max - x_min
    outlier_mask = np.zeros(mask_len, dtype=np.int32)
    for i in range(outlier_indices.shape[0]):
        outlier_mask[outlier_indices[i] - x_min] = 1

    outliers = np.empty(mask_len, dtype=np.int32)
    non_outliers = np.empty(mask_len, dtype=np.int32)
    outlier_count = 0
    non_outlier_count = 0

    # identify outliers and non-outliers
    for i in range(mask_len):
        x = x_min + i
        if outlier_mask[i]:
            outliers[outlier_count] = x
            outlier_count += 1
        else:
            non_outliers[non_outlier_count] = x
            non_outlier_count += 1

    for i in range(outlier_count):
        outlier_idx = outliers[i]

        if non_outlier_count > 0:
            # Step 1: compute distances
            distances = np.empty(non_outlier_count, dtype=np.int32)
            for j in range(non_outlier_count):
                distances[j] = abs(non_outliers[j] - outlier_idx)

            # Step 2: argsort distances
            sorted_indices = np.argsort(distances)

            # Step 3: take up to 4 closest neighbors
            neighbor_count = min(4, non_outlier_count)
            valid_neighbors = np.empty(neighbor_count, dtype=np.int32)
            for k in range(neighbor_count):
                valid_neighbors[k] = non_outliers[sorted_indices[k]]

            # Step 4: apply correction
            diffs = y_values[1, valid_neighbors] - y_values[0, valid_neighbors]
            mean_diff = np.mean(diffs)
            new_y_values[1, outlier_idx] = y_values[0, outlier_idx] + mean_diff

    return new_y_values


# @njit
def error_detect_and_replace(xTmin, xTmax, xRHmin, xRHmax, y, threshold):
    outliers_T = simple_time_analysis(xTmin, xTmax, y, threshold)
    outliers_RH = simple_time_analysis(xRHmin, xRHmax, y, threshold)
    substituted_y_values_T = substitute_outliers(xTmin, xTmax, y, outliers_T)
    substituted_y_values_T_RH = substitute_outliers(
        xRHmin, xRHmax, substituted_y_values_T, outliers_RH
    )
    return outliers_T, substituted_y_values_T_RH[1, :]
