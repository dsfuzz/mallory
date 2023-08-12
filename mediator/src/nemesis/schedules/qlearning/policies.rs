
use rand::{distributions::Uniform, Rng};

use super::{ActionPolicy, QVector, ActionId};


/// Epsilon-greedy policy chooses the "best" action with
/// probability 1-eps and a random action with probability eps.
/// Epsilon is thus a measure of how much exploration we do.
pub struct EpsilonGreedyPolicy {
    epsilon: f64,
    num_actions: usize,
}

pub const DEFAULT_EPSILON: f64 = 0.2;

impl EpsilonGreedyPolicy {
    pub fn new(epsilon: f64, num_actions: usize) -> Self {
        Self {
            epsilon,
            num_actions,
        }
    }
}

impl ActionPolicy for EpsilonGreedyPolicy {
    fn choose_action(&self, q_row: &QVector) -> ActionId {
        let mut rng = rand::thread_rng();
        let unif = Uniform::new(0.0, 1.0);
        let rand = rng.sample(unif);

        if rand < self.epsilon {
            // Exploration: choose a random action.
            let random_action: ActionId = rng.gen_range(0..self.num_actions);
            random_action
        } else {
            // Exploitation: choose the action with the highest Q-value.
            let (best_action, _) = q_row.argmax();
            best_action
        }
    }
}

/// Softmax transforms the accumulated Q-values into a probability distribution.
pub struct SoftmaxPolicy {
    temperature: f64,
    num_actions: usize,
}

pub const DEFAULT_TEMPERATURE: f64 = 100.0;

impl SoftmaxPolicy {
    pub fn new(temperature: f64, num_actions: usize) -> Self {
        Self {
            temperature,
            num_actions,
        }
    }

    /// Turn the vector into a normalized probability distribution.
    pub fn softmax(&self, q_row: &QVector, temperature: f64) -> QVector {
        // https://jdhao.github.io/2022/02/27/temperature_in_softmax/
        let row = q_row.clone();
        let sum_exp = row.map(|x| (x / temperature).exp()).sum();
        row.map(|x| (x / temperature).exp() / sum_exp)
    }
}

impl ActionPolicy for SoftmaxPolicy {
    fn choose_action(&self, q_row: &QVector) -> ActionId {
        let q_values = q_row.iter().cloned().collect::<Vec<_>>();
        let prob_row = self.softmax(q_row, self.temperature);
        let prob_values = prob_row.iter().cloned().collect::<Vec<_>>();

        // We sample from the Q-value row using the inversion method:
        // (https://en.wikipedia.org/wiki/Inverse_transform_sampling)
        // We generate a uniform sample in [0, 1) and then find the first column
        // where the CDF (sum of all prev. probabilities) is greater than the sampled value.
        let mut rng = rand::thread_rng();
        let unif = Uniform::new(0.0, 1.0);
        let sample = rng.sample(unif);
        let mut cdf = 0.0;
        for (action, prob) in prob_row.iter().enumerate() {
            cdf += prob;
            if sample < cdf {
                log::info!(
                    "[SOFTMAX]\nQ-values: {:?}\nProbabilities: {:?}\nSampled action: {}",
                    q_values,
                    prob_values,
                    action
                );

                return action;
            }
        }
        // Should never happen.
        panic!("SoftMaxPolicy: choose_action: no action chosen");
    }
}
