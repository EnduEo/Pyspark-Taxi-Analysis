# %%
from bs4 import BeautifulSoup
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean,stddev
import custom_pckgs.webscrapingFuncs as _scraping

# %%
#Link in format link = "https://xyz.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet"
def get_date_from_link(link): 
    date_str = link.split('_')[-1].split('.')[0]#Gets YYYY-MM from the link
    split_date = date_str.split('-')
    year = split_date[0]
    month = split_date[1]
    year_month = int(year + month) #Integer date in format YYYYMM
    return year_month

# %%
def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m")####YYYY-MM Format expected
    except Exception as e:
        raise Exception(f"Invalid date format. Expected format is YYYY-MM.")

# %%
def download_taxi_files(start_date, end_date):
    validate_date(start_date)
    validate_date(end_date)

    base_url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    yellow_taxi_links = []	
    
    start_date_int = int(start_date.replace('-', '')) #Converts to YYYYMM integer
    end_date_int = int(end_date.replace('-', '')) #Converts to YYYYMM integer
    
    page = _scraping.page_requester(base_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    file_table = soup.find('div', class_='faq-v1')# Gets table where all files are listed
    yearly_files=file_table.find_all('div', class_='faq-answers') #Gets all files year by year
    for year in yearly_files:
        a_tags = year.find_all('a', href=True)
        for a_tag in a_tags:
            link = a_tag['href'] #Filters file links for yellow taxis between the date interval
            if 'yellow_tripdata' in link and (start_date_int <= get_date_from_link(link) <= end_date_int):
                clean_link = link.strip()#Some links have trailing spaces
                _scraping.download_file(clean_link)
                yellow_taxi_links.append(clean_link)
    return yellow_taxi_links

# %%
yellow_taxi_links = download_taxi_files('2024-10', '2024-12')

# %%
#Initiating SparkSession and dataset_path
spark = SparkSession.builder.appName("SparkExercise").getOrCreate()
dataset_path = "./dataset/"

# %%
df = spark.read.parquet(dataset_path)

# %% [markdown]
# Data exploration

# %%
df.printSchema()

# %%
df.show()

# %% [markdown]
# Top 10 % trips based on trip_distance

# %%
row_count = df.count()

top10_percent_count = int(row_count * 0.1)

sorted_df = df.orderBy("trip_distance", ascending=False)

#Get top 10% of trips based on trip distance
top10_percent_df = sorted_df.limit(top10_percent_count)

# %%
top10_percent_df.show() #Sample

# %%
#Stats for total_amount column
top10_percent_df.describe('total_amount').show()

# %%
top10_percent_df.filter(top10_percent_df["total_amount"].isNull()).count() #No null values -> 0

# %%
top10_percent_df.filter(top10_percent_df["total_amount"] < 0).count() #Negative and 0 values present, might be due to refunds, promotional discounts or data errors

# %% [markdown]
# Data Quality & Cleaning

# %%
# 0 or negative values will be filtered out as there is not any specification on the dataset about the meaning of these values
positive_total_df = top10_percent_df.filter(top10_percent_df["total_amount"] > 0)

# %%
positive_total_df.describe('total_amount').show() #Stats 
#Total amount might be affected buy outliers as the maximum value is way higher than the avg, so we could filter out if needed.

# %% [markdown]
# ###### Next block is OPTIONAL because high amount might be reasonable with the trip distance(but trip distance values are really high so they could be incorrect)

# %%
avg = positive_total_df.agg(mean("total_amount"))
sttdev = positive_total_df.agg(stddev("total_amount"))

# %%
#Filter outliers based on avg and standard deviation, in this case lower interval does not filter values as we had removed negative values
cleaned_df = positive_total_df.filter(
    (positive_total_df["total_amount"] >= avg.collect()[0][0] - 3 * sttdev.collect()[0][0]) &
    (positive_total_df["total_amount"] <= avg.collect()[0][0] + 3 * sttdev.collect()[0][0])
)

# %% [markdown]
# Final result

# %%
cleaned_df.describe('total_amount').show()


