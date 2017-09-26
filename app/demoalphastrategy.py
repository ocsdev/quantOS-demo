from framework.alphastrategy import AlphaStrategy
import numpy as np


class DemoAlphaStrategy(AlphaStrategy):
    def init_from_config(self, props):
        super(DemoAlphaStrategy, self).init_from_config(props)
        pass

    def calc_weights(self):
        univ = self.context.universe
        n = len(univ)
        w0 = np.random.rand(n) + 1e-5
        # w = np.ones(n, dtype=float) / n
        w = w0 / w0.sum()
        self.weights = w

