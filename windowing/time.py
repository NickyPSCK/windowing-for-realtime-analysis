import time
import math
import copy
from typing import List, Tuple, Any


class SlidingTimeWindows:
    '''The class to represent a sliding time windows data structure.

    :param duration_size: Window duration (second)
    :type duration_size: float
    :param period_size: Window period (second)
    :type period_size: float

    :raises ValueError: If duration_size cannot divisible by period_size.

    :rtype: SlidingTimeWindows
    :return: SlidingTimeWindows instance

    :Example:

        >>> SW = SlidingTimeWindows(duration_size=6, period_size=2)
        >>> for i in range(13):
        >>>     new_windows_status, window = SW.add(i)
        >>>     time.sleep(1)
        >>>     print('Is new window: ', new_windows_status)
        >>>     print('Periods: ', SW.get_updated_window_periods())
        >>>     print('Window: ', SW.get_updated_window())
        >>>     print('State Window: ', window)
        >>> for i in range(13):
        >>>     time.sleep(1)
        >>>     print('Periods: ', SW.get_updated_window_periods())
        >>>     print('Window: ', SW.get_updated_window())
        >>>     print('State Window: ', window)
        >>> for i in range(13):
        >>>     new_windows_status, window = SW.add(i)
        >>>     time.sleep(1)
        >>>     print('Is new window: ', new_windows_status)
        >>>     print('Periods: ', SW.get_updated_window_periods())
        >>>     print('Window: ', SW.get_updated_window())
        >>>     print('State Window: ', window)

    :Notes:
    A sliding time window also represents time intervals in the data stream;
    however, sliding time windows can overlap. For example, each window might capture 60 seconds worth of data,
    but a new window starts every 30 seconds. The frequency with which sliding windows begin is called the period.
    Therefore, our example would have a window duration of 60 seconds and a period of 30 seconds

    .. seealso::
    This class implemented by using the concepts in the link below:
    https://beam.apache.org/documentation/programming-guide/#sliding-time-windows
    '''

    def __init__(self,
                 duration_size: float,
                 period_size: float):

        no_period = duration_size / period_size
        if no_period - math.floor(no_period) != 0:
            raise ValueError('duration_size must be divisible by period_size')

        self._duration_size = duration_size
        self._period_size = period_size
        self._no_of_retain_periods = int(self._duration_size / self._period_size)

        self._window_periods = self._no_of_retain_periods * [[]]
        self._period = list()

        self._new_window_status = False
        self._start_sliding_window_time = time.time()
        self._start_period_time = self._start_sliding_window_time

    def _update_window_periods(self,
                               time_diff: float,
                               value: Any = None) -> Tuple[List[List], list]:

        tmp_window_periods = copy.deepcopy(self._window_periods)
        tmp_period = copy.deepcopy(self._period)

        if time_diff < (2 * self._period_size):
            tmp_window_periods.append(tmp_period)
            tmp_window_periods.pop(0)
            tmp_period = [value]

        else:  # time_diff >= (2 * self._period_size)

            # Add current window period
            tmp_window_periods.append(tmp_period)
            tmp_window_periods.pop(0)

            # Update blank period
            num_blank_period = time_diff / self._period_size
            floor_num_blank_period = int(math.floor(num_blank_period))

            for _ in range(floor_num_blank_period - 1):
                tmp_window_periods.append(list())
                tmp_window_periods.pop(0)

            # Add new value
            tmp_period = [value]

        return tmp_window_periods, tmp_period

    def get_state_status(self) -> dict:
        '''Returns instance status.

        :rtype: dict
        :return: instance status
        '''
        return {'start_sliding_window_time': self._start_sliding_window_time,
                'start_period_time': self._start_period_time,
                'buffer_period': self._period,
                'window_periods': self._window_periods,
                'window': sum(self._window_periods, [])}

    def get_updated_window_periods(self) -> List[List]:
        '''Returns updated window periods.

        :rtype: List[List]
        :return: updated window periods
        '''
        time_diff = time.time() - self._start_period_time
        if time_diff < self._period_size:
            return self._window_periods
        else:
            # self._window_periods, self._period = self._update_window_periods(time_diff=time_diff, value=None)
            # return self._window_periods
            tmp_window_periods, _ = self._update_window_periods(time_diff=time_diff, value=None)
            return tmp_window_periods

    def get_updated_window(self) -> list:
        '''Returns updated window

        :rtype: list
        :return: updated window
        '''
        tmp_window_periods = self.get_updated_window_periods()
        tmp_window = sum(tmp_window_periods, [])
        return tmp_window

    def add(self,
            value: Any) -> Tuple[List[List], list]:
        '''Returns Tuple which contains "new_window_status" and "window" respectively

        :param value: Value to be added to the window.
        :type value: Any

        :rtype: Tuple[List[List], list]
        :return: Tuple which contains "new_window_status" and "window" respectively

        .. notes::
        "new_window_status" is "True" when the new window is created at the first time.
        '''
        self._new_window_status = False
        time_diff = time.time() - self._start_period_time

        if time_diff < self._period_size:
            self._period.append(value)
        else:
            self._start_period_time = time.time()
            self._window_periods, self._period = self._update_window_periods(time_diff=time_diff, value=value)

            if (time.time() - self._start_sliding_window_time) > self._duration_size:
                self._new_window_status = True

        window = sum(self._window_periods, [])

        return self._new_window_status, window


class FixedTimeWindows(SlidingTimeWindows):
    def __init__(self,
                 duration_size: float):
        super().__init__(duration_size=duration_size,
                         period_size=duration_size)
