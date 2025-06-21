import pandas as pd
from sqlalchemy import create_engine
import os
import matplotlib.pyplot as plt
import seaborn as sns
from calendar import month_name
import plotly.express as px
from scipy import stats
os.environ["PGCLIENTENCODING"] = "UTF-8"


def load_ab_test_data(engine):
    query = """
    WITH order_totals AS (
      SELECT 
        "Order ID",
        "Region",
        SUM("Sales") AS order_total
      FROM superstore
      WHERE "Region" IN ('East', 'West')
      GROUP BY "Order ID", "Region"
    )
    SELECT * FROM order_totals
    """
    return pd.read_sql(query, engine)


def run_ab_test(df):
    group_a = df[df['Region'] == 'East']['order_total']
    group_b = df[df['Region'] == 'West']['order_total']
    
    _, p_a = stats.shapiro(group_a.sample(min(5000, len(group_a)), random_state=42))
    _, p_b = stats.shapiro(group_b.sample(min(5000, len(group_b)), random_state=42))
    
    if p_a > 0.05 and p_b > 0.05:
        test_name = "T-тест"
        t_stat, p_value = stats.ttest_ind(group_b, group_a, equal_var=False)
    else:
        test_name = "U-тест Манна-Уитни"
        u_stat, p_value = stats.mannwhitneyu(group_b, group_a)
    
    effect = group_b.mean() - group_a.mean()
    effect_pct = (effect / group_a.mean()) * 100
    
    print(f"""\nРезультаты A/B теста ({test_name}):
    - Регион East: {len(group_a)} заказов, средний чек = ${group_a.mean():.2f}
    - Регион West: {len(group_b)} заказов, средний чек = ${group_b.mean():.2f}
    - Разница: ${effect:.2f} ({effect_pct:.1f}%)
    - p-value: {p_value:.4f}""")
    
    plt.figure(figsize=(12, 6))
    
    plt.subplot(1, 2, 1)
    sns.boxplot(x='Region', y='order_total', data=df, showfliers=False)
    plt.title('Распределение суммы заказов\n(без выбросов)')
    plt.ylabel('Сумма заказа ($)')
    
    plt.subplot(1, 2, 2)
    sns.histplot(data=df, x='order_total', hue='Region', 
                 bins=30, kde=True, alpha=0.6, palette=['#1f77b4', '#ff7f0e'])
    plt.title(f'Разница средних: ${effect:.2f}\n(p-value: {p_value:.3f})')
    plt.xlabel('Сумма заказа ($)')
    
    plt.tight_layout()
    plt.show()



login = ""
password = ""
database_name = "sales_analysis"

engine = create_engine(f'')
df_orders = load_ab_test_data(engine)
run_ab_test(df_orders)