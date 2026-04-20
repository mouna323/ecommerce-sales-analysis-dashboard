# import required libraries
import duckdb as dd
import pandas as pd


#load data
df = pd.read_csv("Superstore.csv", encoding="latin1")

#cleaning 
df.dropna(inplace=True)
df.columns = df.columns.str.strip() #remove spaces between the column names in the begaining and ending
print(df.shape)

#connect with duckdb
conn=dd.connect()

#register the table 
conn.register("data_table", df)

#explore the data
head = ("""
select * from data_table
limit 5
""")
head_show=conn.execute(head).df()
print(head_show)

print(df.columns)

#core analysis

#1.Sales_by_category
query1=(""" select
        Category, sum(sales) as total_sales,
        rank() over (order by sum(Sales) desc) as rank
        from data_table
        group by Category
        """)


#total profit by category
query2= (""" select category
         ,sum(profit) as total_profit
         from data_table 
         group by category
            order by total_profit desc
            """)

   
#top 10 products by sales
query3=( """ select
        "Product Name", sum(Sales) as total_sales
        from data_table
        group by "Product Name"
        order by total_sales desc
        limit 10
        """)


#sales by region
query4=( """ select
        Region, sum(Sales) as total_sales
        from data_table
        group by Region
        order by total_sales desc
        """)
#sales by segment
query5=( """ select 
        segment, sum(Sales) as total_sales
        from data_table
        group by segment
        order by total_sales desc""")

#salprofitses by segment
query6=( """ select 
        segment, sum(Profit) as total_profits
        from data_table
        group by segment
        order by total_profits desc""")

#sales by ship mode
query7=( """ select
        "Ship Mode", sum(Sales) as total_sales
        from data_table
        group by "Ship Mode"
        order by total_sales desc""")

#Top 10 customers by sales
query8=( """ select
        "Customer Name", sum(Sales) as total_sales
        from data_table
        group by "Customer Name"
        order by total_sales desc
        limit 10""")   


#excute the queries and print the results


def run_query(query, title, save=False):
    df = conn.execute(query).df()
    
    print(f"\n--- {title} ---")
    print(df)
    
    if save:
        file_name = title.lower().replace(" ", "_") + ".csv"
        df.to_csv(f"viz_folder/{file_name}", index=False)
        
    return df



run_query(query1, "Sales by Category",save=True)
run_query(query2, "Total Profit by Category",save=True)
run_query(query3, "Top 10 Products by Sales",save=True)
run_query(query4, "Sales by Region" ,save=True)
run_query(query5, "Sales by Segment",save=True)
run_query(query6, "profits by Segment",save=True)
run_query(query7, "Sales by Ship Mode",save=True)
run_query(query8, "Top 10 Customers by Sales",save=True)


#Insights and recommendations:
#1.most sales category is technology , it should receive more strategic focus, including marketing and inventory optimization
#2.there is mismatch between sales and profit for furniture and office supply , its recommended to optimize pricing, reduce costs, or prioritize higher profit category
#3.best sales of products followed category ,top 10 all from technology , recommended to xpand similar product 
#4.west and east regions get better sales the other two ,recommended to Strength logistics and marketing efforts in these regions
#5.consumer segment is the most profitable one, recommended to focus on customer retention and targeted campaigns
#6.profits by segment followed sales trend across segment , recommended to Scale strategies in high-performing segments
#7.standerd ship achgenerate highest sales maybe due to low cost,recommendede to maintain and optimize standard shipping operations
#8.small group of customers drives large portion of sales recommended to implement loyalty programs and personalized offers.
 