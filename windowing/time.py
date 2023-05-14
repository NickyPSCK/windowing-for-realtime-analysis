import time
from time import strftime, localtime
import math
import copy
from typing import List, Tuple, Any


class SlidingTimeWindows:
    '''The class to represent a sliding time windows data structure.

    :param window_duration: Window duration (second)
    :type window_duration: float
    :param window_period: Window period (second)
    :type window_period: float

    :raises ValueError: If window_duration cannot divisible by window_period.

    :rtype: SlidingTimeWindows
    :return: SlidingTimeWindows instance

    :Example:

        >>> SW = SlidingTimeWindows(window_duration=6, window_period=2)
        >>> for i in range(13):
        >>>     new_windows_status, window = SW.add(i)
        >>>     time.sleep(1)
        >>>     print('Is new window: ', new_windows_status)
        >>>     print('Periods: ', SW.get_current_window_periods())
        >>>     print('Window: ', SW.get_current_window())
        >>>     print('State Window: ', window)
        >>> for i in range(13):
        >>>     time.sleep(1)
        >>>     print('Periods: ', SW.get_current_window_periods())
        >>>     print('Window: ', SW.get_current_window())
        >>>     print('State Window: ', window)
        >>> for i in range(13):
        >>>     new_windows_status, window = SW.add(i)
        >>>     time.sleep(1)
        >>>     print('Is new window: ', new_windows_status)
        >>>     print('Periods: ', SW.get_current_window_periods())
        >>>     print('Window: ', SW.get_current_window())
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
                 window_duration: float,
                 window_period: float):

        no_period = window_duration / window_period
        if no_period - math.floor(no_period) != 0:
            raise ValueError('window_duration must be divisible by window_period')

        self._window_duration = window_duration
        self._window_period = window_period
        self._no_of_retain_periods = int(self._window_duration / self._window_period)

        self._window_periods = self._no_of_retain_periods * [[]]
        self._period = list()

        # Initialize Time
        self.__start_time = time.time()
        self.__start_period_time = self.__start_time

    def _add_new_window_period(self,
                               time_diff: float,
                               value: Any = None) -> Tuple[List[List], list]:

        tmp_window_periods = copy.deepcopy(self._window_periods)
        tmp_period = copy.deepcopy(self._period)

        # Add current window period
        tmp_window_periods.append(tmp_period)
        tmp_window_periods.pop(0)

        if time_diff >= (2 * self._window_period):
            # Update blank period
            num_blank_period = time_diff / self._window_period
            floor_num_blank_period = int(math.floor(num_blank_period))
            for _ in range(floor_num_blank_period - 1):
                tmp_window_periods.append(list())
                tmp_window_periods.pop(0)
        else:  # time_diff < (2 * self._window_period)
            pass

        # Add new value
        if value is not None:
            tmp_period = [value]
        else:
            tmp_period = list()

        return tmp_window_periods, tmp_period

    def get_status(self) -> dict:
        '''Returns instance status.

        :rtype: dict
        :return: instance status
        '''
        return {'start_time': self.__start_time,
                'start_period_time': self.__start_period_time}

    def _calculate_window_boundary(self, start_period_time):
        period_start = strftime('%Y-%m-%d %H:%M:%S',
                                localtime(start_period_time - self._window_period * self._no_of_retain_periods))
        period_end = strftime('%Y-%m-%d %H:%M:%S',
                              localtime(start_period_time))
        window_boundary = (period_start, period_end)
        return window_boundary

    def get_current_window_periods(self) -> List[List]:
        '''Returns current window periods.

        :rtype: List[List]
        :return: current window periods
        '''
        current_time = time.time()
        time_diff = current_time - self.__start_period_time

        if time_diff < self._window_period:
            current_window_periods = self._window_periods
            current_start_period_time = self.__start_period_time
        else:
            current_window_periods, _ = self._add_new_window_period(time_diff=time_diff,
                                                                    value=None)
            no_of_passed_periods = time_diff // self._window_period
            current_start_period_time = self.__start_period_time + (self._window_period * no_of_passed_periods)

        current_window_boundary = self._calculate_window_boundary(current_start_period_time)

        return current_window_periods, current_window_boundary

    def get_current_window(self) -> list:
        '''Returns current window

        :rtype: list
        :return: current window
        '''
        current_window_periods, _ = self.get_current_window_periods()
        current_window = sum(current_window_periods, [])
        return current_window

    def _add(self,
             value: Any = None) -> Tuple[List[List], list]:
        current_time = time.time()
        time_diff = current_time - self.__start_period_time

        if time_diff < self._window_period:
            if value is not None:
                self._period.append(value)
            add_to_new_window = False
        else:
            self._window_periods, self._period = self._add_new_window_period(time_diff=time_diff,
                                                                             value=value)
            no_of_passed_periods = time_diff // self._window_period
            self.__start_period_time += (self._window_period * no_of_passed_periods)
            add_to_new_window = True

        current_window_boundary = self._calculate_window_boundary(self.__start_period_time)
        current_window = sum(self._window_periods, [])
        return add_to_new_window, self._window_periods, current_window, current_window_boundary

    def add(self,
            value: Any = None) -> Tuple[List[List], list]:
        '''Returns Tuple which contains "new_window_status" and "window" respectively

        :param value: Value to be added to the window.
        :type value: Any

        :rtype: Tuple[List[List], list]
        :return: Tuple which contains "new_window_status" and "window" respectively

        .. notes::
        "new_window_status" is "True" when the value is added to newly created window at first time.
        '''
        add_to_new_window, _, current_window, current_window_boundary = self._add(value=value)
        return add_to_new_window, current_window, current_window_boundary


class FixedTimeWindows(SlidingTimeWindows):
    def __init__(self,
                 window_duration: float):
        super().__init__(window_duration=window_duration,
                         window_period=window_duration)
