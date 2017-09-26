import numpy as np

from framework.alphastrategy import AlphaStrategy


class DemoAlphaStrategy(AlphaStrategy):
    def init_from_config(self, props):
        super(DemoAlphaStrategy, self).init_from_config(props)
        pass
    
    def calc_weights(self):
        univ = self.context.universe
        n = len(univ)
        w0 = np.random.rand(n) + 1e-5
        # w = np.ones(n, dtype=float) / n
        
        rand_pos = np.random.randint(0, n)
        
        w0[rand_pos] = 0.0
        w = w0 / w0.sum()
        
        self.weights = w
