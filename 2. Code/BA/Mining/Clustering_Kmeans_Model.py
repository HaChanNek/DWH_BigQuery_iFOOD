# Pip lib
# import lib
import os
from google.cloud import bigquery
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/datawarehouse-422504-39505bda63f7.json'
# Create client for BQ
def train_model():
    client = bigquery.Client(project='datawarehouse-422504')

# Query from BigQuery
    query = """
    SELECT User_ID, Recency, Complain_Times, NumWebVisitsMonth, Total_Purchases, Total_Spent, Overall_Accept_Campaign, NumDealsPurchases, Response
    FROM `datawarehouse-422504.OLAP.fact_MarketingCampaignResponse`
    """
    df = client.query(query).to_dataframe()

    # Processing data
    X = df[
        ['Recency', 'Complain_Times', 'NumWebVisitsMonth', 'Total_Purchases', 'Total_Spent', 'Overall_Accept_Campaign',
         'NumDealsPurchases']]

    # data standard
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # optimize cluster_num ussing elbow
    wcss = []
    for i in range(1, 11):
        kmeans = KMeans(n_clusters=i, init='k-means++', max_iter=300, n_init=10, random_state=42)
        kmeans.fit(X_scaled)
        wcss.append(kmeans.inertia_)

    # elbow_graph
    plt.figure(figsize=(10, 5))
    plt.plot(range(1, 11), wcss, marker='o')
    plt.title('Elbow Method')
    plt.xlabel('Number of clusters')
    plt.ylabel('WCSS')
    plt.show()

    # train model KMeans with optimized cluster_num
    kmeans = KMeans(n_clusters=3, init='k-means++', max_iter=300, n_init=10, random_state=42)
    y_kmeans = kmeans.fit_predict(X_scaled)

    # add in label to DataFrame
    df['Cluster'] = y_kmeans

    # Cluster_summary
    cluster_summary = df.groupby('Cluster').mean()
    print("Cluster Summary:\n", cluster_summary)

    # visual
    sns.pairplot(df, hue='Cluster',
                 vars=['Recency', 'Complain_Times', 'NumWebVisitsMonth', 'Total_Purchases', 'Total_Spent',
                       'Overall_Accept_Campaign', 'NumDealsPurchases'])
    plt.show()
    # Push result back to BigQuery
    # Table existed or not
    table_id = "datawarehouse-422504.OLAP.Clustered_Customers"

    try:
        client.get_table(table_id)
        print("Existed. Replaced.")
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    except:
        print("Not exist. Create new.")
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print("Successfull")
