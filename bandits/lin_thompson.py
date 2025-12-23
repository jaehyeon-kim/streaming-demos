import numpy as np
import matplotlib

# Use a non-interactive backend to be safe, especially on servers
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# =============================================================================
# Linear Thompson Sampling Algorithm Implementation
# =============================================================================
class LinearThompsonSampling:
    """
    Implements the Linear Thompson Sampling contextual bandit algorithm.
    """

    def __init__(self, n_actions, n_features, v=1.0):
        """Initializes the Linear Thompson Sampling agent."""
        self.n_actions = n_actions
        self.n_features = n_features
        self.v = v  # Hyperparameter controlling exploration

        self.B = [np.identity(n_features) for _ in range(n_actions)]
        self.f = [np.zeros((n_features, 1)) for _ in range(n_actions)]

    def select_action(self, context):
        """
        Selects an action by sampling from the posterior distribution of the weights.
        """
        p = np.zeros(self.n_actions)
        for a in range(self.n_actions):
            B_inv = np.linalg.inv(self.B[a])
            mu_a = B_inv.dot(self.f[a])

            # Sample a weight vector `theta_sample` from the posterior distribution.
            try:
                # We need to ensure the covariance matrix is positive semi-definite.
                # A small jitter can help with numerical stability issues.
                cov = self.v**2 * B_inv
                cov = (cov + cov.T) / 2  # Ensure symmetry
                theta_sample = np.random.multivariate_normal(mu_a.ravel(), cov)
            except np.linalg.LinAlgError:
                # If the matrix is not positive semi-definite, fall back to the mean.
                # This is a practical safeguard against numerical instability.
                theta_sample = mu_a.ravel()

            # The result of the dot product is a 1-element array (e.g., array([0.5])).
            # We use .item() to extract the scalar value (0.5) before assigning it to p[a].
            p[a] = context.T.dot(theta_sample).item()

        # Select and return the action with the highest predicted reward from the samples.
        return np.argmax(p)

    def update(self, action, context, reward):
        """
        Updates the model's parameters (B and f) using the observed reward.
        """
        self.B[action] += context.dot(context.T)
        self.f[action] += context * reward


# =============================================================================
# Simulation Environment
# =============================================================================
if __name__ == "__main__":
    # --- Simulation Parameters ---
    N_TRIALS = 5000
    N_ACTIONS = 4
    N_FEATURES = 10
    V_PARAM = 1.5  # Exploration parameter for Thompson Sampling

    # --- 1. Setup the Simulated "Real World" ---
    true_thetas = [np.random.randn(N_FEATURES, 1) for _ in range(N_ACTIONS)]

    # --- 2. Initialize the Agent and Data Trackers ---
    bandit = LinearThompsonSampling(N_ACTIONS, N_FEATURES, v=V_PARAM)
    rewards = []  # List to store reward (0 or 1) for each trial

    # --- 3. Run the Simulation Loop ---
    for t in range(N_TRIALS):
        context = np.random.randn(N_FEATURES, 1)
        context = context / np.linalg.norm(context)

        # Environment side: Calculate the TRUE reward probability for EACH action
        true_reward_probs = [
            1 / (1 + np.exp(-context.T @ theta)) for theta in true_thetas
        ]

        # Agent side: The bandit selects an action
        chosen_action = bandit.select_action(context)

        # Environment side: Simulate a click (reward=1) or no-click (reward=0)
        reward = np.random.binomial(1, true_reward_probs[chosen_action])

        # Agent side: The agent updates its model
        bandit.update(chosen_action, context, reward)

        # Performance Tracking: Store the reward for this trial
        rewards.append(reward)

    # --- 4. Analyze and Plot Results (CORRECTED SECTION) ---
    # Calculate cumulative (running total) of rewards.
    cumulative_rewards = np.cumsum(rewards)

    print(f"Total trials: {N_TRIALS}")
    print(f"Total rewards received (clicks): {np.sum(rewards)}")
    print(f"Overall Click-Through Rate (CTR): {np.sum(rewards) / N_TRIALS:.4f}")

    # Create a figure to hold our plots
    plt.figure(figsize=(12, 5))

    # Plot 1: Cumulative Reward over Time
    plt.subplot(1, 2, 1)
    plt.plot(cumulative_rewards)
    plt.title("Cumulative Reward over Time (Thompson Sampling)")
    plt.xlabel("Trial")
    plt.ylabel("Total Reward")
    plt.grid(True)
    # This curve should consistently increase, and ideally, its slope will increase
    # over time as the agent gets better at finding rewards.

    # Plot 2: Click-Through Rate over Time (using a moving average)
    moving_avg_rewards = [np.mean(rewards[i - 100 : i]) for i in range(100, N_TRIALS)]
    plt.subplot(1, 2, 2)
    plt.plot(moving_avg_rewards)
    plt.title("Click-Through Rate (100-trial Moving Average)")
    plt.xlabel("Trial")
    plt.ylabel("CTR")
    plt.grid(True)
    plt.ylim(0, 1)  # CTR is always between 0 and 1.
    # A good result shows this curve trending upwards.

    # Adjust layout to prevent titles/labels from overlapping
    plt.tight_layout()

    # Save the figure to a file
    output_filename = "thompson_sampling_reward_results.png"
    plt.savefig(output_filename)
    print(f"\nPlot saved to {output_filename}. Open this file to see the results.")
