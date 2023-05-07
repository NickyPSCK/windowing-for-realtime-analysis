import time
from windowing.time import SlidingTimeWindows

if __name__ == '__main__':
    # ------------------------------------------------------------------------------
    # Example
    # ------------------------------------------------------------------------------
    SW = SlidingTimeWindows(duration_size=6, period_size=6)
    for i in range(13):
        print('\n')
        new_windows_status, window = SW.add(i)
        time.sleep(1)
        print('Is new window: ', new_windows_status)
        print('Periods: ', SW.get_updated_window_periods())
        print('Window: ', SW.get_updated_window())
        print('State Window: ', window)

    for i in range(13):
        print('\n')
        time.sleep(1)
        print('Periods: ', SW.get_updated_window_periods())
        print('Window: ', SW.get_updated_window())
        print('State Window: ', window)

    for i in range(13):
        print('\n')
        new_windows_status, window = SW.add(i)
        time.sleep(1)
        print('Is new window: ', new_windows_status)
        print('Periods: ', SW.get_updated_window_periods())
        print('Window: ', SW.get_updated_window())
        print('State Window: ', window)
