import numpy as np
import joblib
from sklearn.ensemble import IsolationForest

# Generate synthetic training data
np.random.seed(42)
normal_data = np.random.normal(loc=100, scale=20, size=(1000, 2))  # Normal transactions
fraud_data = np.random.normal(loc=300, scale=50, size=(50, 2))  # Fraudulent transactions
X_train = np.vstack((normal_data, fraud_data))

# Train Isolation Forest Model
model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
model.fit(X_train)

# Save the trained model
joblib.dump(model, "/scripts/isolation_forest_model.pkl")

print("Model trained and saved successfully.")