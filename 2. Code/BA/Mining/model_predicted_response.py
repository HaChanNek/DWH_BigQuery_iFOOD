from google.cloud import bigquery
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, classification_report
from google.cloud.exceptions import NotFound
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/datawarehouse-422504-39505bda63f7.json'

def predict_model():
# Create client for BigQuery
    client = bigquery.Client(project='datawarehouse-422504')

# Query data from BigQuery
    query = """
    SELECT User_ID, Recency, Complain_Times, NumWebVisitsMonth, Total_Purchases, Total_Spent, Overall_Accept_Campaign, NumDealsPurchases, Response
    FROM `datawarehouse-422504.OLAP.fact_MarketingCampaignResponse`
    """
    df = client.query(query).to_dataframe()

# Data processing
    X = df[['User_ID', 'Recency', 'Complain_Times', 'NumWebVisitsMonth', 'Total_Purchases', 'Total_Spent',
        'Overall_Accept_Campaign', 'NumDealsPurchases']]
    y = df['Response']

# split train/test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# save User_ID
    X_train_user_id = X_train['User_ID']
    X_test_user_id = X_test['User_ID']

# user id not included in train/test
    X_train = X_train.drop(columns=['User_ID'])
    X_test = X_test.drop(columns=['User_ID'])

# data standard
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

# Decision Trees
    tree_model = DecisionTreeClassifier(random_state=42)
    tree_model.fit(X_train_scaled, y_train)

# predict on test
    y_pred = tree_model.predict(X_test_scaled)

# Evaluate
    accuracy = accuracy_score(y_test, y_pred)
    print("Accuracy:", accuracy)
    print("Classification Report:\n", classification_report(y_test, y_pred))

# Data frame for train
    df_results_train = pd.DataFrame({
        'User_ID': X_train_user_id.values,
        'Recency': X_train['Recency'].values,
        'Complain_Times': X_train['Complain_Times'].values,
        'NumWebVisitsMonth': X_train['NumWebVisitsMonth'].values,
        'Total_Purchases': X_train['Total_Purchases'].values,
        'Total_Spent': X_train['Total_Spent'].values,
        'Overall_Accept_Campaign': X_train['Overall_Accept_Campaign'].values,
        'NumDealsPurchases': X_train['NumDealsPurchases'].values,
        'Actual_Response': y_train.values,
        'Predicted_Response': tree_model.predict(X_train_scaled).astype(int)
    })

# DataFrame for test
    df_results_test = pd.DataFrame({
        'User_ID': X_test_user_id.values,
        'Recency': X_test['Recency'].values,
        'Complain_Times': X_test['Complain_Times'].values,
        'NumWebVisitsMonth': X_test['NumWebVisitsMonth'].values,
        'Total_Purchases': X_test['Total_Purchases'].values,
        'Total_Spent': X_test['Total_Spent'].values,
        'Overall_Accept_Campaign': X_test['Overall_Accept_Campaign'].values,
        'NumDealsPurchases': X_test['NumDealsPurchases'].values,
        'Actual_Response': y_test.values,
        'Predicted_Response': y_pred.astype(int)
    })

# Combine train test sets
    df_results = pd.concat([df_results_train, df_results_test])

    table_id = "datawarehouse-422504.OLAP.Predicted_Responses"
    try:
        client.get_table(table_id)
        print("Existed. Replaced.")
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    except NotFound:
        print("Create new table")
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

    job = client.load_table_from_dataframe(df_results, table_id, job_config=job_config)
    job.result()

    print("Successful")