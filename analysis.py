import pymongo
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import probplot
from pandas.plotting import scatter_matrix
from scipy import stats

def change_data():
    client = pymongo.MongoClient("mongodb://localhost:27017/",
                                         username='user',
                                         password='pass',
                                         authSource="admin",
                                         authMechanism="SCRAM-SHA-256")

    db=client["my-database"]
    collection=db["my-collection"]

    cursor=collection.find()

    df=pd.DataFrame(list(cursor))

    return df

def add_describe_to_mongodb(df):
    describe_output=df.describe()

    grouped = df.groupby(['year', 'make']).size().reset_index(name='count')

    grouped_state = df.groupby(['state', 'make']).size().reset_index(name='count') #Hangi lokasyonda hangi model ne kaç adet satıldığını verir

    max_sales = grouped.loc[grouped.groupby('year')['count'].idxmax()]
    
    all_state_data = {
        "state_data":grouped_state.to_dict(orient='records')
    }

    all_data = {
    "max_sales": max_sales.to_dict(orient='records')
        }

    client = pymongo.MongoClient("mongodb://localhost:27017/",
                                         username='user',
                                         password='pass',
                                         authSource="admin",
                                         authMechanism="SCRAM-SHA-256")

    db=client["my-database"]
    collection=db["my-collection"]

    max_sales_collection=db["max-sales-collection"]

    collection.insert_one({'name':'clean_df_describe','data':describe_output.to_dict()})
    max_sales_collection.insert_one(all_data)
    max_sales_collection.insert_one(all_state_data)

def clean_df(df):

    df['make'].fillna('Other',inplace =True)
    df['model'].fillna('Other',inplace =True)
    df['trim'].fillna('Other',inplace =True)
    df['color'].fillna('Other',inplace =True)

    df['condition'].fillna(df['condition'].median(), inplace=True)
    df['odometer'].fillna(df['odometer'].mean(), inplace=True)
    df['mmr'].fillna(df['mmr'].mean(), inplace=True)
    
    #fill with 'Mode'
    df['body'].fillna(df.body.mode()[0], inplace=True)
    df['transmission'].fillna(df['transmission'].mode()[0], inplace=True)
    df['interior'].fillna(df['interior'].mode()[0], inplace=True)
    
    #remove null values
    df.dropna(subset=['vin'], inplace=True)
    df.dropna(subset=['saledate'], inplace=True)

    # Remove outliers using Z-score
    numerical_columns = df.select_dtypes(include=['float64','int64']).columns
    z_scores = stats.zscore(df[numerical_columns])
    clean_df = df[(z_scores < 2).all(axis=1)]
    
    return clean_df


if __name__ == '__main__':

    dataframe = change_data()

    df = clean_df(dataframe)

    add_describe_to_mongodb(df)

    grouped = df.groupby(['year', 'make']).size().reset_index(name='count')

    # Her yıl için en çok satılan markayı bul
    max_sales = grouped.loc[grouped.groupby('year')['count'].idxmax()]

    # Görselleştirme
    fig, ax = plt.subplots(figsize=(12, 8))
    grouped.pivot(index='year', columns='make', values='count').plot(kind='bar', stacked=True, ax=ax)
    plt.xlabel('Year')
    plt.ylabel('Count')
    plt.title('Yearly Count of Makes')
    plt.legend(title='Make', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig("./report_png/YearlyCountofMakes")