import numpy as np
from scipy import signal
import matplotlib.pyplot as plt

# Create example signals
comm_duty_cycles = [0.1, 0.2, 0.3, 0.4, 0.5]

def get_tooth(duty_cycle):
    number_of_ones = int(duty_cycle * 100)
    number_of_zeros = 100 - number_of_ones
    return np.concatenate((np.ones(number_of_ones), np.zeros(number_of_zeros)))


for comm_duty_cycle in comm_duty_cycles:
    up = np.ones(int(comm_duty_cycle * 100))
    down = np.zeros(100 - len(up))
    
    idle = np.array([0] * 100)

    # y is 3 bumps, plus 4 bumps worth of zeros
    y = np.concatenate((idle, idle,  idle,  idle,  up, idle, idle, idle, idle))
    x = np.concatenate((idle, idle,  idle,  idle,  up, idle, idle, idle, idle))
    # x = get_tooth(comm_duty_cycle)
    
    y = np.concatenate((y, y, y))
    
    # Compute correlation at different lag amounts
    lags = np.arange(-len(x) + 1, len(y))
    correlation = np.correlate(y, x, mode='full')
    correlation = correlation[lags >= 0]
    
    print("Correlation at different lag amounts:", correlation)
    
    label_suffix = "{}".format(comm_duty_cycle * 100)
    # plt.xlim(0, 400)
    
    correlation = correlation + 800 + (100 * comm_duty_cycle)
    plt.plot(correlation, label=label_suffix)
    
    
plt.title('Cross-Correlation of Two Signals')
plt.ylabel('Cross-Correlation')
plt.xlabel('Lag')

plt.legend(loc='upper left', bbox_to_anchor=(1.05, 1), title='Comm. Duty Cycle')
    
plt.savefig("plots/xcorr/cross-correlation.png", 
                bbox_inches='tight', dpi=300)

