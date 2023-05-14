import time
from windowing.time import SlidingTimeWindows

if __name__ == '__main__':
    # ------------------------------------------------------------------------------
    # Example
    # ------------------------------------------------------------------------------
    SW = SlidingTimeWindows(window_duration=6, window_period=3)
    for i in range(13):
        time.sleep(1)
        print('\n')
        print('Add:     ', SW.add(i))
        print('Periods: ', SW.get_current_periods())
        print('Window:  ', SW.get_current_window())

    for i in range(16):
        time.sleep(1)
        print('\n')
        print('Periods: ', SW.get_current_periods())
        print('Window:  ', SW.get_current_window())

    for i in range(13):
        print('\n')
        print('Add:     ', SW.add(i))
        time.sleep(1)
        print('Periods: ', SW.get_current_periods())
        print('Window:  ', SW.get_current_window())

