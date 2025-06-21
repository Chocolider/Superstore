import pandas as pd
from sqlalchemy import create_engine
import os
import matplotlib.pyplot as plt
from calendar import month_name
import plotly.express as px
from scipy import stats
os.environ["PGCLIENTENCODING"] = "UTF-8"


    
def get_top_categories(login, password, database_name, metadata_name, months_in_top_count, engine):
	query = f"""
	WITH monthly_top_categories AS (
  SELECT 
    EXTRACT(YEAR FROM TO_DATE("Order Date", 'MM/DD/YYYY')) AS year,
    EXTRACT(MONTH FROM TO_DATE("Order Date", 'MM/DD/YYYY')) AS month,
    "Category",
    "Sub-Category",
    "Product Name",
    SUM("{metadata_name}") AS total,
    ROW_NUMBER() OVER (
      PARTITION BY 
        EXTRACT(YEAR FROM TO_DATE("Order Date", 'MM/DD/YYYY')),
        EXTRACT(MONTH FROM TO_DATE("Order Date", 'MM/DD/YYYY'))
      ORDER BY SUM("{metadata_name}") DESC
    ) AS rank
  FROM superstore
  GROUP BY 
    year, month, "Category", "Sub-Category", "Product Name"
),

product_ranking AS (
  SELECT
    "Category",
    "Sub-Category",
    "Product Name",
    SUM(total) AS product_total,
    ROW_NUMBER() OVER (PARTITION BY "Category", "Sub-Category" ORDER BY SUM(total) DESC) AS product_rank
  FROM monthly_top_categories
  WHERE rank <= 5
  GROUP BY "Category", "Sub-Category", "Product Name"
),

category_performance AS (
  SELECT 
    "Category",
    "Sub-Category",
    COUNT(DISTINCT year || '-' || month) AS months_in_top,
    COUNT(DISTINCT "Product Name") AS unique_top_products,
    (
      SELECT ARRAY_AGG("Product Name")
      FROM (
        SELECT "Product Name"
        FROM product_ranking pr
        WHERE pr."Category" = mtc."Category" 
          AND pr."Sub-Category" = mtc."Sub-Category"
        ORDER BY product_rank
        LIMIT 3
      ) top_products
    ) AS top_product_examples
  FROM monthly_top_categories mtc
  WHERE rank <= 5
  GROUP BY "Category", "Sub-Category"
  HAVING COUNT(DISTINCT year || '-' || month) >= {months_in_top_count}
)

SELECT 
  "Category",
  "Sub-Category",
  months_in_top,
  unique_top_products,
  top_product_examples,
  ROUND(months_in_top * 100.0 / 48, 1) AS percentage_time_in_top
FROM category_performance
ORDER BY months_in_top DESC, unique_top_products DESC;
	"""

	top_categories = pd.read_sql(query, engine)
	return top_categories

def plot_results(df, metadata_name, months_in_top_count):
	if metadata_name == "Sales":
		fig1 = px.pie(
	    df.groupby('Category', as_index=False).agg({'months_in_top': 'sum'}),
	    values='months_in_top',
	    names='Category',
	    title=f'Доля категорий товаров, которые были наиболее популярны более {months_in_top_count} месяцев суммарно в период с 2014 по 2017',
	    hole=0.4,
	    color_discrete_sequence=px.colors.qualitative.Pastel,
	    width=1000,
        height=700
	)
	elif metadata_name == "Profit":
		fig1 = px.pie(
	    df.groupby('Category', as_index=False).agg({'months_in_top': 'sum'}),
	    values='months_in_top',
	    names='Category',
	    title=f'Доля категорий товаров, которые приносили наибольшую прибыль более {months_in_top_count} месяцев суммарно в период с 2014 по 2017',
	    hole=0.4,
	    color_discrete_sequence=px.colors.qualitative.Pastel
	)

	fig1.update_traces(
	    textposition='inside',
	    textinfo='percent+label',
	    pull=[0.05 for _ in range(len(df))]
	)
	fig1.show()

	top_examples = df.explode('top_product_examples').head(10)  

	if metadata_name == "Sales":
		fig2 = px.sunburst(
		    top_examples,
		    path=['Category', 'Sub-Category', 'top_product_examples'],
		    values='months_in_top',
		    title='Примеры стабильно популярных товаров по категориям',
		    color='Category'
		)
		
	elif metadata_name == "Profit":
		fig2 = px.sunburst(
		    top_examples,
		    path=['Category', 'Sub-Category', 'top_product_examples'],
		    values='months_in_top',
		    title='Примеры стабильно прибыльных товаров по категориям',
		    color='Category'
		)
	fig2.show()





login = ""
password = ""
database_name = "sales_analysis"
metadata_name = ["Sales", "Profit"]
#print(metadata_name[0])
months_in_top_count = 24 
engine = create_engine(f'')
df = get_top_categories(login, password, database_name, metadata_name[1], months_in_top_count, engine)
plot_results(df, metadata_name[1], months_in_top_count)

