# Import necessary libraries
import numpy as np
import matplotlib

# Set the backend for matplotlib to 'Agg'.
# This is a non-interactive backend that allows you to create and save plots
# in environments without a graphical user interface (like a server terminal).
# This prevents the "FigureCanvasAgg is non-interactive" warning.
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# =============================================================================
# LinUCB Algorithm Implementation
# =============================================================================
class LinUCB:
    """
    Implements the Linear Upper Confidence Bound (LinUCB) algorithm.

    LinUCB is a contextual bandit algorithm that models the expected reward of
    an action as a linear function of the context. It uses the UCB principle
    to balance exploration (trying new actions to learn about them) and
    exploitation (choosing the action that is currently estimated to be the best).

    Attributes:
        n_actions (int): The number of possible actions (arms).
        n_features (int): The dimensionality of the context feature vectors.
        alpha (float): A parameter that controls the level of exploration.
                       A higher alpha encourages more exploration of uncertain arms.
        A (list of np.array): A list where each element A[i] is a matrix used to
                              calculate the reward prediction for action i.
                              It's essentially (X_i^T * X_i + I), where X_i are
                              the contexts seen for action i. It is initialized
                              to the identity matrix for numerical stability.
        b (list of np.array): A list where each element b[i] is a vector used to
                              calculate the reward prediction for action i.
                              It's essentially (X_i^T * y_i), where y_i are the
                              rewards received for action i.
    """

    def __init__(self, n_actions, n_features, alpha=1.0):
        """Initializes the LinUCB agent."""
        # --- Algorithm Parameters ---
        self.n_actions = n_actions  # Number of actions to choose from
        self.n_features = n_features  # Dimension of the context vectors
        self.alpha = alpha  # Exploration-exploitation trade-off parameter

        # --- Model Parameters for Each Action ---
        # Initialize A as a list of identity matrices (one for each action).
        # The identity matrix is added for regularization (similar to Ridge Regression),
        # which helps in making the matrix inversion stable, especially at the beginning.
        # Shape of each A[i]: (n_features, n_features)
        self.A = [np.identity(n_features) for _ in range(n_actions)]

        # Initialize b as a list of zero vectors (one for each action).
        # This vector will accumulate the rewards weighted by their corresponding contexts.
        # Shape of each b[i]: (n_features, 1)
        self.b = [np.zeros((n_features, 1)) for _ in range(n_actions)]

    def select_action(self, context):
        """
        Selects an action for a given context using the UCB formula.
        The formula is: UCB_score = expected_reward + uncertainty_bonus
        """
        # `p` will store the calculated UCB score for each action.
        p = np.zeros(self.n_actions)

        # Iterate through each action to calculate its score.
        for a in range(self.n_actions):
            # --- Core of the LinUCB calculation ---

            # 1. Invert the A matrix for the current action.
            # A_inv is crucial for both estimating the reward and calculating uncertainty.
            A_inv = np.linalg.inv(self.A[a])

            # 2. Estimate the coefficient vector (theta) for the action's linear model.
            # This is the solution to a regularized least-squares problem (Ridge Regression).
            # theta_a represents the learned relationship between context and reward for action 'a'.
            theta_a = A_inv.dot(self.b[a])

            # --- Calculate the two parts of the UCB score ---

            # 3. Part 1: EXPLOITATION (Estimated Reward)
            # Predict the reward for this action by taking the dot product of the estimated
            # coefficients (theta_a) and the current context.
            # This is the "exploit" part: our best guess for the reward right now.
            # .item() converts the resulting 1x1 matrix (e.g., [[0.7]]) into a plain number (0.7).
            estimated_reward = theta_a.T.dot(context).item()

            # 4. Part 2: EXPLORATION (Uncertainty Bonus)
            # This term quantifies the uncertainty of our reward estimate.
            # It is proportional to the standard deviation of the reward estimate.
            # - `context.T.dot(A_inv).dot(context)` gives the variance of the prediction.
            # - The square root turns it into the standard deviation.
            # - `alpha` scales this uncertainty, controlling how much we value exploration.
            # A larger bonus is given to actions where the context is unfamiliar.
            uncertainty_bonus = (
                self.alpha * np.sqrt(context.T.dot(A_inv).dot(context)).item()
            )

            # 5. Combine exploitation and exploration to get the final UCB score.
            p[a] = estimated_reward + uncertainty_bonus

        # Select and return the action with the highest UCB score.
        # This action is either promising (high estimated reward) or uncertain (high bonus),
        # perfectly balancing the trade-off.
        return np.argmax(p)

    def update(self, action, context, reward):
        """
        Updates the model's parameters (A and b) for the chosen action based on the observed reward.
        This is the learning step of the algorithm.
        """
        # 1. Update the A matrix for the chosen action.
        # We add the outer product of the context vector with itself.
        # This effectively adds information about the features of the context we've just seen.
        # Over time, `A` accumulates the covariance of the contexts for this action.
        self.A[action] += context.dot(context.T)

        # 2. Update the b vector for the chosen action.
        # We add the context vector scaled by the observed scalar reward.
        # This update strengthens the learned relationship (theta) between the context
        # features and the reward. If a feature was present and the reward was high,
        # its corresponding value in `b` will increase.
        self.b[action] += reward * context


# =============================================================================
# Simulation Environment
# =============================================================================
if __name__ == "__main__":
    # --- Simulation Parameters ---
    N_TRIALS = 5000
    N_ACTIONS = 4
    N_FEATURES = 10
    ALPHA = 1.5  # Exploration parameter for LinUCB

    # --- 1. Setup the Simulated "Real World" ---
    true_thetas = [np.random.randn(N_FEATURES, 1) for _ in range(N_ACTIONS)]

    # --- 2. Initialize the Agent and Data Trackers ---
    bandit = LinUCB(N_ACTIONS, N_FEATURES, alpha=ALPHA)
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
    plt.title("Cumulative Reward over Time (LinUCB)")
    plt.xlabel("Trial")
    plt.ylabel("Total Reward")
    plt.grid(True)
    # This curve should consistently increase.

    # Plot 2: Click-Through Rate over Time (using a moving average)
    moving_avg_rewards = [np.mean(rewards[i - 100 : i]) for i in range(100, N_TRIALS)]
    plt.subplot(1, 2, 2)
    plt.plot(moving_avg_rewards)
    plt.title("Click-Through Rate (100-trial Moving Average)")
    plt.xlabel("Trial")
    plt.ylabel("CTR")
    plt.grid(True)
    plt.ylim(0, 1)
    # A good result shows this curve trending upwards.

    # Adjust layout to prevent titles/labels from overlapping
    plt.tight_layout()

    # Save the figure to a file
    output_filename = "linucb_reward_results.png"
    plt.savefig(output_filename)
    print(f"\nPlot saved to {output_filename}. Open this file to see the results.")
