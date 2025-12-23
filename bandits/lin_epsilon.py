import numpy as np
import matplotlib

# Use a non-interactive backend to be safe, especially on servers
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# =============================================================================
# Contextual Epsilon-Greedy Algorithm Implementation
# =============================================================================
class ContextualEpsilonGreedy:
    """
    Implements the Contextual Epsilon-Greedy bandit algorithm.

    This version adapts the simple Epsilon-Greedy strategy to a contextual setting.
    The "greedy" action is chosen by predicting the reward for each action based on
    the current context using a linear model, rather than just using the historical average.

    Attributes:
        n_actions (int): The number of possible actions (arms).
        n_features (int): The dimensionality of the context feature vectors.
        epsilon (float): The probability of choosing a random action (exploration).
        A (list of np.array): A list where each element A[i] is a matrix used for
                              the ridge regression of action i's linear model.
        b (list of np.array): A list where each element b[i] is a vector used for
                              the ridge regression of action i's linear model.
    """

    def __init__(self, n_actions, n_features, epsilon=0.1):
        """Initializes the Contextual Epsilon-Greedy agent."""
        if not (0.0 <= epsilon <= 1.0):
            raise ValueError("Epsilon must be between 0 and 1.")

        self.n_actions = n_actions
        self.n_features = n_features
        self.epsilon = epsilon

        # Initialize parameters for the linear model of each action
        # This is the same setup as LinUCB for performing ridge regression
        self.A = [np.identity(n_features) for _ in range(n_actions)]
        self.b = [np.zeros((n_features, 1)) for _ in range(n_actions)]

    def select_action(self, context):
        """
        Selects an action using the contextual Epsilon-Greedy strategy.
        """
        # --- The Core of the Epsilon-Greedy Logic ---
        if np.random.rand() < self.epsilon:
            # EXPLORE: Choose a random action.
            return np.random.randint(self.n_actions)
        else:
            # EXPLOIT: Choose the best action based on the current context.
            # We predict the reward for each action using its linear model.

            p = np.zeros(self.n_actions)
            for a in range(self.n_actions):
                # Calculate the estimated weight vector (theta) for this action's model
                A_inv = np.linalg.inv(self.A[a])
                theta_a = A_inv.dot(self.b[a])

                # Predict the reward using the linear model: context^T * theta
                p[a] = context.T.dot(theta_a).item()

            # Return the action with the highest predicted reward.
            return np.argmax(p)

    def update(self, action, context, reward):
        """
        Updates the linear model for the chosen action based on the observed reward.
        """
        # This update is identical to LinUCB's update
        self.A[action] += context.dot(context.T)
        self.b[action] += reward * context


# =============================================================================
# Simulation Environment (Identical to previous examples)
# =============================================================================
if __name__ == "__main__":
    # --- Simulation Parameters ---
    N_TRIALS = 5000
    N_ACTIONS = 4
    N_FEATURES = 10
    EPSILON = 0.1  # 10% exploration probability

    # --- 1. Setup the Simulated "Real World" ---
    true_thetas = [np.random.randn(N_FEATURES, 1) for _ in range(N_ACTIONS)]

    # --- 2. Initialize the Agent and Data Trackers ---
    bandit = ContextualEpsilonGreedy(N_ACTIONS, N_FEATURES, epsilon=EPSILON)
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

    # --- 4. Analyze and Plot Results ---
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
    plt.title("Cumulative Reward over Time (Epsilon-Greedy)")
    plt.xlabel("Trial")
    plt.ylabel("Total Reward")
    plt.grid(True)

    # Plot 2: Click-Through Rate over Time (using a moving average)
    moving_avg_rewards = [np.mean(rewards[i - 100 : i]) for i in range(100, N_TRIALS)]
    plt.subplot(1, 2, 2)
    plt.plot(moving_avg_rewards)
    plt.title("Click-Through Rate (100-trial Moving Average)")
    plt.xlabel("Trial")
    plt.ylabel("CTR")
    plt.grid(True)
    plt.ylim(0, 1)

    # Adjust layout to prevent titles/labels from overlapping
    plt.tight_layout()

    # Save the figure to a file
    output_filename = "epsilon_greedy_reward_results.png"
    plt.savefig(output_filename)
    print(f"\nPlot saved to {output_filename}. Open this file to see the results.")
