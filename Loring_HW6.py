#import some modules needed
import matplotlib.pyplot as plt
import numpy as np
from numpy.random import default_rng
from sklearn import linear_model


#defines the class
class SLR_slope_simulator:
    
    #creates initial attributes per instructions
    def __init__(self, beta_0, beta_1, x, sigma, seed): 
        self.beta_0 = beta_0
        self.beta_1 = beta_1
        self.x = np.array(x)
        self.sigma = sigma
        self.rng = default_rng(seed)
        self.n = len(x)
        self.slopes = []
        
        
    def generate_data(self):
        #creates y values according to SLR formula
        y = self.beta_0 + (self.beta_1 * self.x) + self.rng.normal(0, self.sigma, self.n)
        
        #returns x and y as numpy arrays
        return self.x, np.array(y)
    
    def fit_slope(self, x, y):
        #creates a reg object
        reg = linear_model.LinearRegression()
        
        #fits the model
        mod_fit = reg.fit(x.reshape(-1, 1), y)
        
        #returns the estimated slope value
        return mod_fit.coef_[0]
    
    def run_simulations(self, sim_num):
        # initialize a numpy array of sim_num zeros (faster than using an empty list!)
        self.slopes = np.zeros(sim_num)
        
        #uses a for loop to generate data and fit the slopes for the number of simulations supplied
        for i in range(sim_num):
            x, y = self.generate_data() #supplies x & y arguments together for simulation i
            b1 = self.fit_slope(x, y) #returns slope value for simulation i
            self.slopes[i] = b1 #stores slope value in numpy array for simulation i
    
    def plot_sampling_distribution(self):
        if len(self.slopes) > 0: #plots sampling distribution of slopes if there are slopes to plot
            plt.hist(self.slopes)
            plt.title("Visualizing the Approximation to the Sampling \n Distribution of the Sample Slope")
            plt.xlabel("Estimated Slope Value")
            plt.ylabel("Frequency")
            plt.show() #eliminates printed text output that is not needed
        else: #prints message if user tries to plot before running simulations
            print("run_simulations() must be called first, please try again")
            
    def find_prob(self, value, sided):
        if len(self.slopes) > 0: #must have at least 1 slope to compute probability
            if sided == "above":
                bool_prob = self.slopes > value #formula for "above" probability
                return bool_prob.mean()
            elif sided == "below":
                bool_prob = self.slopes < value #formula for "below" probability
                return bool_prob.mean()
            elif sided == "two-sided":
                median_value = np.median(self.slopes) #computes median value of simulated slopes
                if value > median_value:
                    bool_prob = self.slopes >= value #formula for "two-sided" probability
                    return bool_prob.mean() * 2 #have to multiply by 2 since it's two-sided
                else:
                    bool_prob = self.slopes <= value #formula for "two-sided" probability
                    return bool_prob.mean() * 2 #have to multiply by 2 since it's two-sided
            else:
                raise ValueError("'sided' argument must be 'above', 'below', or 'two-sided'. Please try again.")
        else:
             print("run_simulations() must be called first, please try again")
                           
                
# implementing the class
test = SLR_slope_simulator(beta_0 = 12, beta_1 = 2, x = np.array(list(np.linspace(start = 0, stop = 10, num = 11))*3),
                           sigma = 1, seed = 10) #creates instance of class object with given parameters
test.plot_sampling_distribution() #will return error message since we haven't yet run "run_simulations()"
test.run_simulations(10000) #runs 10000 simulations
test.plot_sampling_distribution() #plots the sampling distribution
test.find_prob(2.1, "two-sided") #approximates the two-sided probability of being larger than 2.1
test.slopes #prints out the values of the simulated slopes using the "slopes" attribute