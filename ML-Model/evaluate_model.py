import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    classification_report
)

df = pd.read_parquet(
    "telemetry-data/predictions/inference_batch_20260628_070013.parquet"
)

# Ground truth
y_true = df["is_anomaly"]

# Isolation Forest output
# prediction: 1=normal, -1=anomaly
y_pred = (df["prediction"] == -1).astype(int)

print("="*60)
print("MODEL EVALUATION")
print("="*60)

print("Accuracy :", accuracy_score(y_true, y_pred))
print("Precision:", precision_score(y_true, y_pred))
print("Recall   :", recall_score(y_true, y_pred))
print("F1 Score :", f1_score(y_true, y_pred))

print("\nConfusion Matrix")
print(confusion_matrix(y_true, y_pred))

print("\nClassification Report")
print(classification_report(y_true, y_pred))